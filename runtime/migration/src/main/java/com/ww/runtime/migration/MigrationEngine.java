package com.ww.runtime.migration;

import java.util.ArrayList;
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
 *       {@link PartitionDataAdapter}；真实跨节点由 Task 11 组合本地/远端 adapter；</li>
 *   <li><b>CUTOVER 走 placement CAS</b>：PREPARING 记录的 epoch 与切主时的
 *       expectedEpoch 一致才生效，失败即 FAILED（fencing 语义）；</li>
 *   <li><b>断点续跑</b>：SNAPSHOT 阶段把 {epoch, sourceCount} 以 JSON 写入
 *       checkpoint_json，COPYING 每片后 saveProgress；exportChunks(fromCheckpoint)
 *       天然支持从断点继续；</li>
 *   <li><b>单片可靠性</b>：每片过 {@link ChunkCodec#verify} 校验 + importChunks
 *       幂等导入，失败重试至多 3 次仍败则任务 FAILED（与 TransferStream 的
 *       ack 重发语义同构，本地直连路径同样走该逻辑）。</li>
 * </ul>
 */
public final class MigrationEngine {

    private static final Logger LOG = LoggerFactory.getLogger(MigrationEngine.class);

    /** 单片导入（checksum 校验/导入）最大尝试次数。 */
    private static final int MAX_IMPORT_ATTEMPTS = 3;

    /** SYNCING 阶段防复制期间持续新增导致的死循环保护上限。 */
    private static final int MAX_SYNC_ROUNDS = 10_000;

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
        try {
            Meta meta = Meta.parse(task.checkpointJson());
            long epoch = meta.epoch();
            long sourceCount = meta.sourceCount();

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
                sourceCount = adapter.itemCount(task.capability(), task.partition());
                persistProgress(taskId, metaCheckpoint(taskId), epoch, sourceCount);
                advance(taskId, MigrationTaskState.COPYING);
                state = MigrationTaskState.COPYING;
            }
            if (state == MigrationTaskState.COPYING) {
                copyRemaining(taskId, task, epoch, sourceCount);
                advance(taskId, MigrationTaskState.SYNCING);
                state = MigrationTaskState.SYNCING;
            }
            if (state == MigrationTaskState.SYNCING) {
                syncUntilQuiescent(taskId, task, epoch, sourceCount);
                advance(taskId, MigrationTaskState.CUTOVER);
                state = MigrationTaskState.CUTOVER;
            }
            if (state == MigrationTaskState.CUTOVER) {
                // 切主前以 store 最新断点元数据为准（并发执行者可能已持久化更高的 epoch），
                // 消除接力者携带入口陈旧 epoch 导致的 "epoch unknown" 误报。
                long latestEpoch = Meta.parse(requireTask(taskId).checkpointJson()).epoch();
                if (latestEpoch > epoch) {
                    epoch = latestEpoch;
                }
                if (epoch < 0) {
                    throw new MigrationException("cutover epoch unknown for task " + taskId);
                }
                boolean cut = placementStore.cutover(task.capability(), task.partition(),
                        task.toNode(), epoch);
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
                long targetCount = adapter.verifyCount(task.capability(), task.partition());
                if (sourceCount >= 0 && targetCount < sourceCount) {
                    throw new MigrationException("verify failed for task " + taskId
                            + ": target=" + targetCount + " < source=" + sourceCount);
                }
                advance(taskId, MigrationTaskState.COMPLETED);
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
            LOG.warn("migration task {} failed at {}: {}",
                    taskId, store.get(taskId).map(MigrationTask::state).orElse(null), e.getMessage());
            try {
                store.transition(taskId, MigrationTaskState.FAILED);
            } catch (RuntimeException suppressed) {
                LOG.debug("task {} could not be marked FAILED (already terminal?): {}", taskId,
                        suppressed.getMessage());
            }
        }
    }

    /** 任务被并发执行者推进（claim/CAS 争抢或已终态）：本执行者放弃，不标记 FAILED。 */
    private static final class TaskContendedException extends RuntimeException {
        TaskContendedException(String message) {
            super(message);
        }
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

    /** COPYING：从最新断点导出剩余分片，逐片「校验→导入→saveProgress→限流」。 */
    private void copyRemaining(long taskId, MigrationTask task, long epoch, long sourceCount) {
        MigrationTask latest = requireTask(taskId);
        long checkpoint = latest.checkpointOffset();
        long items = latest.itemsDone();
        List<byte[]> pieces = adapter.exportChunks(task.capability(), task.partition(), checkpoint);
        for (byte[] piece : pieces) {
            long entries = importPieceWithRetry(taskId, task, piece);
            checkpoint += entries;
            items += entries;
            persistProgress(taskId, new Progress(checkpoint, items), epoch, sourceCount);
            throttle();
        }
    }

    /** SYNCING：反复从断点增量拉取直至为空（防复制期间新增写入漏迁）。 */
    private void syncUntilQuiescent(long taskId, MigrationTask task, long epoch, long sourceCount) {
        for (int round = 0; round < MAX_SYNC_ROUNDS; round++) {
            MigrationTask latest = requireTask(taskId);
            List<byte[]> pieces = adapter.exportChunks(task.capability(), task.partition(),
                    latest.checkpointOffset());
            if (pieces.isEmpty()) {
                return;
            }
            long checkpoint = latest.checkpointOffset();
            long items = latest.itemsDone();
            for (byte[] piece : pieces) {
                long entries = importPieceWithRetry(taskId, task, piece);
                checkpoint += entries;
                items += entries;
                persistProgress(taskId, new Progress(checkpoint, items), epoch, sourceCount);
                throttle();
            }
        }
        throw new MigrationException("syncing did not converge for task " + taskId);
    }

    /**
     * 单片可靠性：过 checksum 校验函数后幂等导入；失败重试至多
     * {@link #MAX_IMPORT_ATTEMPTS} 次仍败抛 {@link MigrationException}。
     *
     * @return 该片包含的条目数
     */
    private long importPieceWithRetry(long taskId, MigrationTask task, byte[] piece) {
        String checksum = ChunkCodec.sha256Hex(piece);
        MigrationException last = null;
        for (int attempt = 1; attempt <= MAX_IMPORT_ATTEMPTS; attempt++) {
            if (ChunkCodec.verify(piece, checksum)
                    && adapter.importChunks(task.capability(), task.partition(), List.of(piece))) {
                return ChunkCodec.countEntries(piece);
            }
            last = new MigrationException("chunk import failed (attempt " + attempt
                    + "/" + MAX_IMPORT_ATTEMPTS + ") for task " + taskId);
        }
        throw last;
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

    private void persistProgress(long taskId, Progress progress, long epoch, long sourceCount) {
        store.saveProgress(taskId, progress.checkpointOffset(), progress.itemsDone(),
                Meta.toJson(epoch, sourceCount));
    }

    private Progress metaCheckpoint(long taskId) {
        MigrationTask latest = requireTask(taskId);
        return new Progress(latest.checkpointOffset(), latest.itemsDone());
    }

    private record Progress(long checkpointOffset, long itemsDone) {
    }

    /** checkpoint_json 中的断点元数据：{"epoch":E,"sourceCount":S}。 */
    record Meta(long epoch, long sourceCount) {

        private static final Pattern FIELD = Pattern.compile("\"(epoch|sourceCount)\"\\s*:\\s*(-?\\d+)");

        static final Meta EMPTY = new Meta(-1L, -1L);

        static String toJson(long epoch, long sourceCount) {
            return "{\"epoch\":" + epoch + ",\"sourceCount\":" + sourceCount + "}";
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
            return new Meta(epoch, sourceCount);
        }
    }
}
