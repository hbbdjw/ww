package com.ww.runtime.migration;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ww.runtime.common.Clock;
import com.ww.runtime.common.IllegalStateTransitionException;
import com.ww.runtime.common.MigrationException;
import com.ww.runtime.common.MigrationTaskState;
import com.ww.runtime.common.PlacementRole;
import com.ww.runtime.placement.Placement;
import com.ww.runtime.placement.PlacementStore;
import com.ww.runtime.placement.RebalancePlanner.PartitionMove;

/**
 * 迁移编排引擎（存储无关，spec R6）：驱动单任务走完
 * PENDING→PREPARING→SNAPSHOT→COPYING→SYNCING→CUTOVER→VERIFYING→COMPLETED 状态机，
 * 每步跃迁经 store CAS 落库、断点进度逐步持久化，节点重启后
 * {@link #resumeAll()} 按 checkpointOffset 续跑。
 *
 * <p>设计要点：</p>
 * <ul>
 *   <li><b>不依赖 scheduler 模块</b>：以 {@link #migrateAndWait} 提供 scheduler
 *       期望的「提交一批 PartitionMove 并等待结果」语义；</li>
 *   <li><b>源与目标同 adapter</b>：本阶段同进程 Mock 场景，源读/目标写走同一
 *       {@link PartitionDataAdapter}；真实跨节点由本地/远端 adapter 组合而成；</li>
 *   <li><b>CUTOVER 走 placement CAS</b>：PREPARING 记录的 epoch 与切主时的
 *       expectedEpoch 一致才生效，失败即 FAILED（fencing 语义）；目标节点已持有该分区
 *       REPLICA 行时（缩容排空/治愈降级的结构性场景）改走副本提升——直接 cutover 会
 *       违反放置主键 (capability, partition_id, node_id)；</li>
 *   <li><b>断点续跑</b>：SNAPSHOT 阶段把 {epoch, sourceCount} 以 JSON 写入
 *       checkpoint_json，COPYING 按片批量 saveProgress（批大小
 *       {@link #PROGRESS_PERSIST_BATCH}，失败/收尾/轮末必持久）；
 *       exportChunks(fromCheckpoint) 天然支持从断点继续；</li>
 *   <li><b>单片可靠性</b>：每片 SHA-256 只算一次并随导入传递（接收端
 *       TransferStream 端到端校验）+ importChunks 幂等导入，失败重试至多
 *       3 次仍败则任务 FAILED（与 TransferStream 的 ack 重发语义同构，
 *       本地直连路径同样走该逻辑）。</li>
 * </ul>
 */
public final class MigrationEngine {

    private static final Logger LOG = LoggerFactory.getLogger(MigrationEngine.class);

    /** 单片导入（导入重试）最大尝试次数。 */
    private static final int MAX_IMPORT_ATTEMPTS = 3;

    /** 进度断点落库频率（片数）：每批片持久化一次进度，避免每片一次 DB UPDATE。 */
    private static final int PROGRESS_PERSIST_BATCH = 16;

    /** SYNCING 阶段防复制期间持续新增导致的死循环保护上限。 */
    private static final int MAX_SYNC_ROUNDS = 10_000;

    /** 迁移期源分区冻结续期间隔（TTL 默认 5 分钟，30s 续期对崩溃窗口留足余量）。 */
    private static final long FREEZE_RENEW_INTERVAL_MILLIS = 30_000L;

    private final MigrationTaskStore store;
    private final PartitionDataAdapter adapter;
    private final PlacementStore placementStore;
    private final MigrationConfig config;
    private final Clock clock;

    public MigrationEngine(MigrationTaskStore store,
                           PartitionDataAdapter adapter,
                           PlacementStore placementStore,
                           MigrationConfig config,
                           Clock clock) {
        this.store = Objects.requireNonNull(store, "store");
        this.adapter = Objects.requireNonNull(adapter, "adapter");
        this.placementStore = Objects.requireNonNull(placementStore, "placementStore");
        this.config = Objects.requireNonNull(config, "config");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * 提交一批分区迁移并等待全部结束：每个 move 建任务（同 capability+partition+to
     * 的<b>活动</b>任务复用不重复建），经 Semaphore 限并发（虚拟线程池执行）。
     *
     * @return 全部 COMPLETED 返回 true；任一 FAILED（其余继续跑完）返回 false
     */
    public boolean migrateAndWait(String capability, List<PartitionMove> moves) {
        Objects.requireNonNull(capability, "capability");
        Objects.requireNonNull(moves, "moves");
        List<Long> taskIds = new ArrayList<>(moves.size());
        for (PartitionMove move : moves) {
            taskIds.add(obtainTask(move));
        }
        runAll(taskIds);
        boolean allCompleted = true;
        for (Long taskId : taskIds) {
            Optional<MigrationTask> task = store.get(taskId);
            boolean completed = task.map(t -> t.state() == MigrationTaskState.COMPLETED).orElse(false);
            if (!completed) {
                LOG.info("migration task {} final state: {}", taskId,
                        task.map(MigrationTask::state).orElse(null));
            }
            allCompleted &= completed;
        }
        return allCompleted;
    }

    /**
     * 启动恢复：listActive 的任务按最新 checkpointOffset 续跑；
     * FAILED 任务经 FAILED→PENDING 重试跃迁后同样续跑（断点保留）。
     */
    public void resumeAll() {
        List<Long> activeIds = store.listActive().stream().map(MigrationTask::taskId).toList();
        runAll(activeIds);
        List<Long> retryIds = store.listAll().stream()
                .filter(t -> t.state() == MigrationTaskState.FAILED)
                .map(MigrationTask::taskId)
                .toList();
        for (Long taskId : retryIds) {
            try {
                // 重置前复核当前状态：多节点共享 DB，各自启动时并发执行 resumeAll，
                // 快照中为 FAILED 的任务可能已被其他节点重置为 PENDING（或推进到终态）。
                // 不复核直接 transition 会产生 PENDING->PENDING 非法跃迁 / CAS 冲突噪声。
                MigrationTask current = store.get(taskId).orElse(null);
                if (current == null || current.state() != MigrationTaskState.FAILED) {
                    continue;
                }
                store.transition(taskId, MigrationTaskState.PENDING);
            } catch (RuntimeException e) {
                // 残余冲突是多节点并发重置的正常现象（CAS 失败即他节点已推进），降级避免刷屏
                LOG.debug("resumeAll: cannot reset task {} to PENDING: {}", taskId, e.getMessage());
            }
        }
        runAll(retryIds);
    }

    /**
     * 取消任务（仅非终态可取消）。
     *
     * @return 取消成功 true；任务已是终态返回 false
     */
    public boolean cancel(long taskId) {
        MigrationTask task = store.get(taskId)
                .orElseThrow(() -> new MigrationException("task not found: " + taskId));
        if (MigrationStateMachine.isTerminal(task.state())) {
            return false;
        }
        store.transition(taskId, MigrationTaskState.CANCELLED);
        return true;
    }

    /** 当前时间（与 store 同一口径注入，便于测试注入 TestClock）。 */
    long nowMillis() {
        return clock.nowMillis();
    }

    // ------------------------------------------------------------------
    // 任务执行：状态机驱动的分阶段推进（对断点续跑友好——按当前状态跳入）
    // ------------------------------------------------------------------

    private void runTask(long taskId) {
        MigrationTask task = store.get(taskId)
                .orElseThrow(() -> new MigrationException("task not found: " + taskId));
        boolean resumed = task.checkpointOffset() > 0;
        boolean progressReset = false;
        FreezeGuard freeze = new FreezeGuard(task);
        try {
            Meta meta = Meta.parse(task.checkpointJson());
            long epoch = meta.epoch();
            long sourceCount = meta.sourceCount();
            String cursor = meta.cursor();

            MigrationTaskState state = task.state();
            if (state == MigrationTaskState.PENDING) {
                advance(taskId, MigrationTaskState.PREPARING);
                state = MigrationTaskState.PREPARING;
            }
            if (state == MigrationTaskState.PREPARING) {
                epoch = resolveEpoch(task);
                advance(taskId, MigrationTaskState.SNAPSHOT);
                state = MigrationTaskState.SNAPSHOT;
            }
            if (state == MigrationTaskState.SNAPSHOT) {
                if (epoch < 0) {
                    // 接力竞态自愈：另一执行者刚把任务推进到 SNAPSHOT 但尚未持久化 epoch 元数据，
                    // 本执行者入口读到 checkpointJson=null（epoch=-1）；按任务 from 重新解析。
                    // from 为 null（引导型 move）时 resolveEpoch 返回 placement 当前 epoch。
                    epoch = resolveEpoch(task);
                }
                sourceCount = adapter.itemCount(task.capability(), task.partition(), task.fromNode());
                MigrationTask latest = requireTask(taskId);
                if (resumed || latest.checkpointOffset() > 0) {
                    // 断点跨越了执行节点崩溃/重试（源端冻结已到期），位置与键游标都不再可信：
                    // 从零重放（导入幂等），保证不遗漏断点之前的写入。
                    persistProgress(taskId, new Progress(0L, 0L), epoch, sourceCount, null);
                    cursor = null;
                    progressReset = true;
                } else {
                    persistProgress(taskId,
                            new Progress(latest.checkpointOffset(), latest.itemsDone()),
                            epoch, sourceCount, cursor);
                }
                advance(taskId, MigrationTaskState.COPYING);
                state = MigrationTaskState.COPYING;
            }

            // 数据阶段。从 SYNCING/CUTOVER 恢复时回退到「复制 + 同步」重放：
            // 崩溃窗口内源端可能已解冻并接受写入，仅续跑游标会漏数据。
            boolean replayFromMidway = resumed && !progressReset
                    && (state == MigrationTaskState.SYNCING || state == MigrationTaskState.CUTOVER);
            if (state == MigrationTaskState.COPYING || replayFromMidway) {
                if (replayFromMidway) {
                    persistProgress(taskId, new Progress(0L, 0L), epoch, sourceCount, null);
                    cursor = null;
                }
                freeze.acquireIfNeeded();
                cursor = copyRemaining(taskId, task, epoch, sourceCount, cursor, freeze);
                if (state == MigrationTaskState.COPYING) {
                    advance(taskId, MigrationTaskState.SYNCING);
                    state = MigrationTaskState.SYNCING;
                }
                cursor = syncUntilQuiescent(taskId, task, epoch, sourceCount, cursor, freeze);
                if (state == MigrationTaskState.SYNCING) {
                    advance(taskId, MigrationTaskState.CUTOVER);
                    state = MigrationTaskState.CUTOVER;
                }
            } else if (state == MigrationTaskState.SYNCING) {
                freeze.acquireIfNeeded();
                cursor = syncUntilQuiescent(taskId, task, epoch, sourceCount, cursor, freeze);
                advance(taskId, MigrationTaskState.CUTOVER);
                state = MigrationTaskState.CUTOVER;
            }
            if (state == MigrationTaskState.CUTOVER) {
                // 切主前确保冻结仍有效（SYNCING 可能耗时较长）
                freeze.renew();
                // 切主前以 store 最新断点元数据为准（并发执行者可能已持久化更高的 epoch），
                // 消除接力者携带入口陈旧 epoch 导致的 "epoch unknown" 误报。
                long latestEpoch = Meta.parse(requireTask(taskId).checkpointJson()).epoch();
                if (latestEpoch > epoch) {
                    epoch = latestEpoch;
                }
                if (epoch < 0) {
                    throw new MigrationException("cutover epoch unknown for task " + taskId);
                }
                boolean cut = cutOverOrPromote(task, epoch);
                if (!cut) {
                    // fencing 作废：分区已被并发执行者切主（纪元已推进）——目标往往已由他人达成，
                    // 启动/重平衡收敛期高频正常事件，降为 INFO 避免噪声。
                    throw new FencedException("cutover CAS failed for task " + taskId
                            + " (" + task.capability() + "/" + task.partition()
                            + "): expected epoch " + epoch + " no longer current");
                }
                advance(taskId, MigrationTaskState.VERIFYING);
                state = MigrationTaskState.VERIFYING;
            }
            if (state == MigrationTaskState.VERIFYING) {
                long targetCount = adapter.verifyCount(task.capability(), task.partition(), task.toNode());
                if (sourceCount >= 0 && targetCount < sourceCount) {
                    throw new MigrationException("verify failed for task " + taskId
                            + ": target=" + targetCount + " < source=" + sourceCount);
                }
                advance(taskId, MigrationTaskState.COMPLETED);
                clearSourcePartition(task);
            }
        } catch (TaskContendedException e) {
            // 多节点共享 DB：另一执行者（leader reconcile / 他节点 resumeAll）已认领并推进本任务，
            // 或任务已进终态。争抢失败≠任务失败：静默放弃且绝不标 FAILED——
            // 否则会把他人正在执行的任务误杀成 FAILED，引发互相标杀的风暴。
            LOG.debug("migration task {} contended by another executor, skipped: {}", taskId, e.getMessage());
        } catch (FencedException e) {
            // fencing 作废（目标已过时：分区已被并发执行者切主/属主已变）：任务标 FAILED 供下轮重试收敛，
            // 但属高频正常事件（启动引导/重平衡收敛期批量出现），降为 INFO 避免噪声。
            LOG.info("migration task {} fenced (target already converged): {}",
                    taskId, e.getMessage());
            try {
                store.transition(taskId, MigrationTaskState.FAILED);
            } catch (RuntimeException suppressed) {
                LOG.debug("task {} could not be marked FAILED (already terminal?): {}", taskId,
                        suppressed.getMessage());
            }
        } catch (RuntimeException e) {
            MigrationTaskState current = store.get(taskId).map(MigrationTask::state).orElse(null);
            if (MigrationStateMachine.isTerminal(current)) {
                // 迟到的重复执行者：任务已被并发执行者（leader 换届后的他节点 reconcile /
                // 他节点 resumeAll）推进到终态——高频场景为对方 COMPLETED 后 TransferStream
                // 以 "already terminal" 拒绝本执行者的迟到分片。终态不可改写，放弃即可，
                // 不改状态不按失败告警（否则打出 "failed at COMPLETED" 假警报）。
                LOG.info("migration task {} duplicate executor abandoning: already terminal ({}): {}",
                        taskId, current, e.getMessage());
                return;
            }
            // 瞬态前置条件失败（端点能力处于启动/冻结窗口，服务端以 FAILED_PRECONDITION 表达）：
            // 断点保留标 FAILED 供下轮 resume/重规划续跑——属滚动重启期的高频正常事件，
            // 降为 INFO 且不打栈，避免告警噪声淹没真错误。
            if (isPreconditionFailure(e)) {
                LOG.info("migration task {} deferred at {}: endpoint capability not ready (startup window), checkpoint kept",
                        taskId, current);
                try {
                    store.transition(taskId, MigrationTaskState.FAILED);
                } catch (RuntimeException suppressed) {
                    LOG.debug("task {} could not be marked FAILED (already terminal?): {}", taskId,
                            suppressed.getMessage());
                }
                return;
            }
            // 异常对象作为末参传入：保留完整堆栈与根因链（如 PSQLException duplicate key），
            // 仅打 getMessage() 会吞掉包装异常的 cause，阻断根因诊断。
            LOG.warn("migration task {} failed at {}", taskId, current, e);
            try {
                store.transition(taskId, MigrationTaskState.FAILED);
            } catch (RuntimeException suppressed) {
                LOG.debug("task {} could not be marked FAILED (already terminal?): {}", taskId,
                        suppressed.getMessage());
            }
        } finally {
            freeze.release();
        }
    }

    /**
     * 源分区冻结守卫：COPYING 前冻结源端写入，批间按 {@link #FREEZE_RENEW_INTERVAL_MILLIS}
     * 续期，任务结束（成功/失败）立即解冻；执行节点崩溃时由 TTL 自愈。
     */
    private final class FreezeGuard {

        private final MigrationTask task;
        private boolean active;
        private long lastRenewAtMono;

        FreezeGuard(MigrationTask task) {
            this.task = task;
        }

        void acquireIfNeeded() {
            if (active || task.fromNode() == null) {
                return;
            }
            if (!adapter.freezePartition(task.capability(), task.partition(), task.fromNode(),
                    config.partitionFreezeTtlMillis())) {
                throw new MigrationException("failed to freeze source partition for migration: "
                        + task.capability() + "/" + task.partition() + " on " + task.fromNode());
            }
            active = true;
            lastRenewAtMono = clock.monotonicMillis();
        }

        void renewIfDue() {
            if (!active) {
                return;
            }
            long now = clock.monotonicMillis();
            if (now - lastRenewAtMono < FREEZE_RENEW_INTERVAL_MILLIS) {
                return;
            }
            renew();
        }

        void renew() {
            if (!active) {
                return;
            }
            if (!adapter.freezePartition(task.capability(), task.partition(), task.fromNode(),
                    config.partitionFreezeTtlMillis())) {
                throw new MigrationException("failed to renew source partition freeze: "
                        + task.capability() + "/" + task.partition());
            }
            lastRenewAtMono = clock.monotonicMillis();
        }

        void release() {
            if (!active) {
                return;
            }
            active = false;
            try {
                adapter.freezePartition(task.capability(), task.partition(), task.fromNode(), 0L);
            } catch (RuntimeException e) {
                // 尽力解冻；失败由 TTL 自愈
                LOG.warn("source partition unfreeze failed (TTL will heal): {} {} on {}: {}",
                        task.capability(), task.partition(), task.fromNode(), e.getMessage());
            }
        }
    }

    /**
     * 瞬态前置条件失败判定：服务端在端点能力处于启动/冻结窗口时以
     * {@code FAILED_PRECONDITION} 应答（fetch/count/transfer 任一路径），
     * 沿 cause 链识别该状态码即视为可稍后重试的延后事件，不记 INTERNAL 噪声。
     */
    private static boolean isPreconditionFailure(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof io.grpc.StatusRuntimeException sre
                    && sre.getStatus().getCode() == io.grpc.Status.Code.FAILED_PRECONDITION) {
                return true;
            }
        }
        return false;
    }

    /** 任务被并发执行者推进（claim/CAS 争抢或已终态）：本执行者放弃，不标记 FAILED。 */
    private static final class TaskContendedException extends RuntimeException {
        TaskContendedException(String message) {
            super(message);
        }
    }

    /**
     * 切主或副本提升：目标节点已持有该分区 REPLICA 行时，直接 cutover 会违反
     * 放置主键 (capability, partition_id, node_id)（排空场景中目标必为旧环第二顺位=
     * 副本持有者，结构性命中），改走 promoteReplica（数据已在目标，无损免迁移）。
     * 返回 false 由调用方按 fencing 语义处理。
     */
    private boolean cutOverOrPromote(MigrationTask task, long epoch) {
        boolean targetHoldsReplica = placementStore.placements(task.capability(), task.partition()).stream()
                .anyMatch(pl -> pl.role() == PlacementRole.REPLICA && pl.node().equals(task.toNode()));
        if (targetHoldsReplica) {
            return placementStore.promoteReplica(task.capability(), task.partition(), task.toNode(), epoch);
        }
        return placementStore.cutover(task.capability(), task.partition(), task.toNode(), epoch);
    }

    /** fencing 作废：任务目标已过时（分区纪元/属主已被并发执行者推进），高频正常事件。 */
    private static final class FencedException extends MigrationException {
        FencedException(String message) {
            super(message);
        }
    }

    /**
     * 状态推进：将 store.transition 的 CAS 冲突 / 非法跃迁转换为 {@link TaskContendedException}，
     * 与业务失败（数据校验、cutover fencing、导入重试耗尽等 MigrationException）区分——
     * 前者意味着「他人已推进此任务」，后者才是「任务失败」。
     */
    private void advance(long taskId, MigrationTaskState to) {
        try {
            store.transition(taskId, to);
        } catch (IllegalStateTransitionException | MigrationException e) {
            throw new TaskContendedException("transition to " + to + " rejected: " + e.getMessage());
        }
    }

    /** PREPARING：校验 placement 当前 owner == from（from 为 null 时仅读取 epoch），返回该 epoch 作为 cutover 的 expectedEpoch。 */
    private long resolveEpoch(MigrationTask task) {
        Optional<Placement> placement = placementStore.get(task.capability(), task.partition());
        if (task.fromNode() == null) {
            return placement.map(Placement::epoch).orElse(-1L);
        }
        Placement current = placement.orElseThrow(() -> new MigrationException(
                "no placement for " + task.capability() + "/" + task.partition()
                        + "; expected owner " + task.fromNode()));
        if (!current.node().equals(task.fromNode())) {
            // fencing 作废：分区属主已非任务快照时的 from（已被并发迁移/重算），高频正常事件。
            throw new FencedException("partition " + task.partition() + " of "
                    + task.capability() + " owned by " + current.node()
                    + ", expected " + task.fromNode());
        }
        return current.epoch();
    }

    /**
     * COPYING：从最新断点流式导出剩余分片，逐片「导入→限流」，进度按
     * {@link #PROGRESS_PERSIST_BATCH} 片批量落库（避免每片一次 DB UPDATE）。
     * 导出为惰性流——拉一片、导一片，堆内恒定单片量级（大分区不物化入堆）。
     */
    private String copyRemaining(long taskId, MigrationTask task, long epoch, long sourceCount,
                                 String cursor, FreezeGuard freeze) {
        MigrationTask latest = requireTask(taskId);
        long checkpoint = latest.checkpointOffset();
        long items = latest.itemsDone();
        Iterator<byte[]> pieces = adapter.exportChunks(task.capability(), task.partition(),
                checkpoint, cursor, task.fromNode());
        int chunksSincePersist = 0;
        try {
            while (pieces.hasNext()) {
                byte[] piece = pieces.next();
                long entries = importPieceWithRetry(taskId, task, piece);
                checkpoint += entries;
                items += entries;
                String pieceCursor = adapter.cursorOf(task.capability(), piece);
                if (pieceCursor != null) {
                    cursor = pieceCursor;
                }
                if (++chunksSincePersist >= PROGRESS_PERSIST_BATCH) {
                    persistProgress(taskId, new Progress(checkpoint, items), epoch, sourceCount, cursor);
                    chunksSincePersist = 0;
                }
                freeze.renewIfDue();
                throttle();
            }
        } catch (RuntimeException e) {
            // 失败前落盘内存中的最新进度：断点停在最后成功导入处，续跑不重拷大段
            persistProgress(taskId, new Progress(checkpoint, items), epoch, sourceCount, cursor);
            throw e;
        } finally {
            // T24：中止/异常路径强制关闭导出流，消灭文件句柄泄漏
            adapter.closeExportChunks(pieces);
        }
        if (chunksSincePersist > 0) {
            // 收尾必持久：SYNCING 断点重读与重启续跑均以此为准
            persistProgress(taskId, new Progress(checkpoint, items), epoch, sourceCount, cursor);
        }
        return cursor;
    }

    /** SYNCING：反复从断点增量流式拉取直至为空（防复制期间新增写入漏迁）。 */
    private String syncUntilQuiescent(long taskId, MigrationTask task, long epoch,
                                      long sourceCount, String cursor, FreezeGuard freeze) {
        for (int round = 0; round < MAX_SYNC_ROUNDS; round++) {
            MigrationTask latest = requireTask(taskId);
            Iterator<byte[]> pieces = adapter.exportChunks(task.capability(), task.partition(),
                    latest.checkpointOffset(), cursor, task.fromNode());
            if (!pieces.hasNext()) {
                adapter.closeExportChunks(pieces);
                return cursor;
            }
            long checkpoint = latest.checkpointOffset();
            long items = latest.itemsDone();
            int chunksSincePersist = 0;
            try {
                do {
                    byte[] piece = pieces.next();
                    long entries = importPieceWithRetry(taskId, task, piece);
                    checkpoint += entries;
                    items += entries;
                    String pieceCursor = adapter.cursorOf(task.capability(), piece);
                    if (pieceCursor != null) {
                        cursor = pieceCursor;
                    }
                    if (++chunksSincePersist >= PROGRESS_PERSIST_BATCH) {
                        persistProgress(taskId, new Progress(checkpoint, items), epoch, sourceCount, cursor);
                        chunksSincePersist = 0;
                    }
                    freeze.renewIfDue();
                    throttle();
                } while (pieces.hasNext());
            } catch (RuntimeException e) {
                persistProgress(taskId, new Progress(checkpoint, items), epoch, sourceCount, cursor);
                throw e;
            } finally {
                // T24：中止/异常路径强制关闭导出流，消灭文件句柄泄漏
                adapter.closeExportChunks(pieces);
            }
            if (chunksSincePersist > 0) {
                // 轮末必持久：下轮从库重读断点判定收敛，陈旧断点会重复拉取同批分片
                persistProgress(taskId, new Progress(checkpoint, items), epoch, sourceCount, cursor);
            }
        }
        throw new MigrationException("syncing did not converge for task " + taskId);
    }

    /**
     * 单片可靠性：每片 SHA-256 仅计算一次并随导入调用传递（传输层直接用作线上
     * checksum，接收端 TransferStream 据此端到端校验），随后幂等导入；失败重试至多
     * {@link #MAX_IMPORT_ATTEMPTS} 次仍败抛 {@link MigrationException}。
     *
     * @return 该片包含的条目数
     */
    private long importPieceWithRetry(long taskId, MigrationTask task, byte[] piece) {
        String checksum = ChunkCodec.sha256Hex(piece);
        MigrationException last = null;
        for (int attempt = 1; attempt <= MAX_IMPORT_ATTEMPTS; attempt++) {
            if (adapter.importChunks(task.capability(), task.partition(),
                    List.of(piece), task.toNode(), taskId, List.of(checksum))) {
                return ChunkCodec.countEntries(piece);
            }
            last = new MigrationException("chunk import failed (attempt " + attempt
                    + "/" + MAX_IMPORT_ATTEMPTS + ") for task " + taskId);
        }
        throw last;
    }

    /**
     * 源端清理（尽力而为）：迁移 COMPLETED 后释放源节点上该分区的数据。
     * cutover 后源端写入已被 placement fencing 拒绝，清理与新写无竞态；
     * 失败（能力未实现清理 / 源端失联）仅告警——任务终态不变，残留数据由
     * 该能力下次启动清扫（retainPartitions）兜底。
     */
    private void clearSourcePartition(MigrationTask task) {
        if (task.fromNode() == null) {
            return; // 无历史归属的新分区，源端无数据可清
        }
        try {
            boolean cleared = adapter.clearPartition(
                    task.capability(), task.partition(), task.fromNode());
            if (cleared) {
                LOG.info("source partition cleared: {} {} on {}", task.capability(),
                        task.partition(), task.fromNode());
            } else {
                LOG.warn("source partition clear returned false: {} {} on {} (left for startup sweep)",
                        task.capability(), task.partition(), task.fromNode());
            }
        } catch (RuntimeException e) {
            // 异常对象作末参：message 为 null 的异常（如 NPE）仅打 getMessage() 会打成 "null"
            LOG.warn("source partition clear failed: {} {} on {} (left for startup sweep)",
                    task.capability(), task.partition(), task.fromNode(), e);
        }
    }

    /** 简单限流：每片后按 chunkSize / maxBytesPerSecond 睡眠。 */
    private void throttle() {
        long sleepMillis = config.throttleMillisPerChunk();
        if (sleepMillis <= 0) {
            return;
        }
        try {
            Thread.sleep(sleepMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MigrationException("migration throttling interrupted", e);
        }
    }

    // ------------------------------------------------------------------
    // 任务提交与并发
    // ------------------------------------------------------------------

    /** 建/复用任务：同 capability+partition+to 的活动任务直接复用。 */
    private long obtainTask(PartitionMove move) {
        return store.listActive().stream()
                .filter(t -> t.capability().equals(move.capability())
                        && t.partition() == move.partition()
                        && t.toNode().equals(move.to()))
                .findFirst()
                .map(MigrationTask::taskId)
                .orElseGet(() -> store.create(move.capability(), move.partition(),
                        move.from(), move.to()));
    }

    /** 并发执行：虚拟线程池 + Semaphore(maxConcurrent)，全部结束后返回。 */
    private void runAll(List<Long> taskIds) {
        if (taskIds.isEmpty()) {
            return;
        }
        Semaphore permits = new Semaphore(config.maxConcurrentTasks());
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (Long taskId : taskIds) {
                executor.submit(() -> {
                    permits.acquireUninterruptibly();
                    try {
                        runTask(taskId);
                    } finally {
                        permits.release();
                    }
                });
            }
        }
    }

    private MigrationTask requireTask(long taskId) {
        return store.get(taskId)
                .orElseThrow(() -> new MigrationException("task not found: " + taskId));
    }

    private void persistProgress(long taskId, Progress progress, long epoch, long sourceCount,
                                 String cursor) {
        store.saveProgress(taskId, progress.checkpointOffset(), progress.itemsDone(),
                Meta.toJson(epoch, sourceCount, cursor));
    }

    private Progress metaCheckpoint(long taskId) {
        MigrationTask latest = requireTask(taskId);
        return new Progress(latest.checkpointOffset(), latest.itemsDone());
    }

    private record Progress(long checkpointOffset, long itemsDone) {
    }

    /**
     * checkpoint_json 中的断点元数据：
     * {@code {"epoch":E,"sourceCount":S[,"cursorB64":"..."]}}。
     * cursorB64 为键型游标的 Base64Url（避免任意 key 字符破坏 JSON）。
     */
    record Meta(long epoch, long sourceCount, String cursor) {

        private static final Pattern FIELD = Pattern.compile(
                "\"(epoch|sourceCount)\"\\s*:\\s*(-?\\d+)");
        private static final Pattern CURSOR = Pattern.compile(
                "\"cursorB64\"\\s*:\\s*\"([A-Za-z0-9_\\-]*)\"");

        static final Meta EMPTY = new Meta(-1L, -1L, null);

        static String toJson(long epoch, long sourceCount, String cursor) {
            StringBuilder json = new StringBuilder()
                    .append("{\"epoch\":").append(epoch)
                    .append(",\"sourceCount\":").append(sourceCount);
            if (cursor != null) {
                json.append(",\"cursorB64\":\"")
                        .append(java.util.Base64.getUrlEncoder().withoutPadding()
                                .encodeToString(cursor.getBytes(java.nio.charset.StandardCharsets.UTF_8)))
                        .append('"');
            }
            return json.append('}').toString();
        }

        static Meta parse(String json) {
            if (json == null || json.isBlank()) {
                return EMPTY;
            }
            Long epoch = null;
            Long sourceCount = null;
            Matcher matcher = FIELD.matcher(json);
            while (matcher.find()) {
                long value = Long.parseLong(matcher.group(2));
                if ("epoch".equals(matcher.group(1))) {
                    epoch = value;
                } else {
                    sourceCount = value;
                }
            }
            if (epoch == null || sourceCount == null) {
                return EMPTY;
            }
            Matcher cursorMatcher = CURSOR.matcher(json);
            String cursor = cursorMatcher.find()
                    ? new String(java.util.Base64.getUrlDecoder().decode(cursorMatcher.group(1)),
                            java.nio.charset.StandardCharsets.UTF_8)
                    : null;
            return new Meta(epoch, sourceCount, cursor);
        }
    }
}
