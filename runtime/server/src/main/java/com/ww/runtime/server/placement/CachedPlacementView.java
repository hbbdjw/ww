package com.ww.runtime.server.placement;

import com.ww.runtime.capability.PlacementView;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.Clock;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.PartitionId;
import com.ww.runtime.common.StaleEpochException;
import com.ww.runtime.placement.Placement;
import com.ww.runtime.placement.PlacementStore;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 数据面放置视图：本地不可变快照 + 修订号轮询。
 *
 * <p>读路由（partition → owner/holders）全部走内存，不再对每个数据面请求查询
 * PostgreSQL。控制面仍是事实源：所有 placement 写路径推进能力修订号，本视图在
 * 后台发现修订号变化后重载该能力快照；另设低频全量刷新兜底混合版本/漏推进等场景。</p>
 *
 * <p>写 fencing 通过 {@link #checkWritable} 在本地完成：owner/epoch 来自快照，
 * {@code validAfterMs} 保证新属主等待旧属主租约过期，最后由 {@link LeaseValidator}
 * 校验本节点持有的分区租约。任一条件不满足均 fail-closed。</p>
 */
public final class CachedPlacementView implements PlacementView, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CachedPlacementView.class);

    /** 刷新异常日志节流：首次与之后每 60 次打印一次。 */
    private static final long FAILURE_LOG_EVERY = 60L;

    private final PlacementStore placementStore;
    private final Clock clock;
    private final List<CapabilityType> types;
    private final long refreshIntervalMs;
    private final long fullRefreshIntervalMs;
    private final long validAfterSafetyMarginMs;

    private final AtomicReference<Snapshot> snapshot = new AtomicReference<>(Snapshot.empty());
    private final AtomicReference<LeaseValidator> leaseValidator = new AtomicReference<>();
    private final AtomicLong consecutiveFailures = new AtomicLong();
    private final AtomicLong refreshCount = new AtomicLong();
    private final AtomicLong reloadCount = new AtomicLong();
    private final Object refreshLock = new Object();

    private volatile Runnable snapshotListener;
    private volatile long lastFullRefreshAt;
    private volatile long dbAnchorDbMillis = -1L;
    private volatile long dbAnchorMonoMillis = -1L;
    private ScheduledExecutorService executor;

    public CachedPlacementView(PlacementStore placementStore,
                               Clock clock,
                               long refreshIntervalMs,
                               long fullRefreshIntervalMs,
                               long validAfterSafetyMarginMs) {
        this.placementStore = Objects.requireNonNull(placementStore, "placementStore");
        this.clock = Objects.requireNonNull(clock, "clock");
        if (refreshIntervalMs <= 0) {
            throw new IllegalArgumentException("refreshIntervalMs must be > 0, got " + refreshIntervalMs);
        }
        if (fullRefreshIntervalMs < refreshIntervalMs) {
            throw new IllegalArgumentException("fullRefreshIntervalMs must be >= refreshIntervalMs");
        }
        if (validAfterSafetyMarginMs < 0) {
            throw new IllegalArgumentException("validAfterSafetyMarginMs must be >= 0");
        }
        this.refreshIntervalMs = refreshIntervalMs;
        this.fullRefreshIntervalMs = fullRefreshIntervalMs;
        this.validAfterSafetyMarginMs = validAfterSafetyMarginMs;
        this.types = List.copyOf(EnumSet.allOf(CapabilityType.class));
    }

    @PostConstruct
    public synchronized void start() {
        if (executor != null) {
            return;
        }
        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "rt-placement-view-refresh");
            t.setDaemon(true);
            return t;
        });
        executor = exec;
        exec.scheduleWithFixedDelay(this::safeRefresh, 0, refreshIntervalMs, TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    public synchronized void stop() {
        ScheduledExecutorService exec = executor;
        executor = null;
        if (exec != null) {
            exec.shutdownNow();
        }
    }

    @Override
    public void close() {
        stop();
    }

    /** 立即同步刷新（测试/管理面变更后可主动调用）。失败时保留上一份快照并抛出。 */
    public void refreshNow() {
        synchronized (refreshLock) {
            refresh();
        }
    }

    @Override
    public Optional<Owner> owner(CapabilityType cap, PartitionId partition) {
        Objects.requireNonNull(cap, "cap");
        Objects.requireNonNull(partition, "partition");
        List<Placement> rows = rows(snapshot.get(), cap, partition.value());
        if (rows.isEmpty()) {
            return Optional.empty();
        }
        Placement primary = rows.getFirst();
        return Optional.of(new Owner(primary.node(), primary.epoch(), primary.validAfterMs()));
    }

    @Override
    public List<Owner> holders(CapabilityType cap, PartitionId partition) {
        Objects.requireNonNull(cap, "cap");
        Objects.requireNonNull(partition, "partition");
        return rows(snapshot.get(), cap, partition.value()).stream()
                .map(row -> new Owner(row.node(), row.epoch(), row.validAfterMs()))
                .toList();
    }

    @Override
    public Map<Integer, List<Owner>> holdersAll(CapabilityType cap) {
        Objects.requireNonNull(cap, "cap");
        Map<Integer, List<Placement>> byPartition = snapshot.get().byCapability().get(cap);
        if (byPartition == null || byPartition.isEmpty()) {
            return Map.of();
        }
        Map<Integer, List<Owner>> result = new HashMap<>(byPartition.size());
        for (Map.Entry<Integer, List<Placement>> entry : byPartition.entrySet()) {
            result.put(entry.getKey(), entry.getValue().stream()
                    .map(row -> new Owner(row.node(), row.epoch(), row.validAfterMs()))
                    .toList());
        }
        return Map.copyOf(result);
    }

    @Override
    public boolean isReady(CapabilityType cap) {
        return snapshot.get().loaded().contains(cap);
    }

    /** 所有能力均完成首次加载（租约管理器据此决定是否开始续约）。 */
    public boolean isReady() {
        Snapshot current = snapshot.get();
        return current.loaded().containsAll(types);
    }

    @Override
    public void refreshRoutingView() {
        try {
            refreshNow();
        } catch (RuntimeException e) {
            LOG.debug("routing view refresh on stale-epoch failed: {}", e.getMessage());
        }
    }

    @Override
    public void checkWritable(CapabilityType cap, PartitionId partition, NodeId self,
                              long expectedEpoch) {
        Snapshot current = snapshot.get();
        if (!current.loaded().contains(cap)) {
            throw new StaleEpochException("placement view not ready for " + cap);
        }
        List<Placement> rows = rows(current, cap, partition.value());
        if (rows.isEmpty()) {
            throw new StaleEpochException(expectedEpoch, -1L);
        }
        Placement primary = rows.getFirst();
        if (!primary.node().equals(self)
                || (expectedEpoch >= 0 && primary.epoch() != expectedEpoch)) {
            throw new StaleEpochException(expectedEpoch, primary.epoch());
        }
        long now = clock.nowMillis();
        long validAfter = primary.validAfterMs();
        // valid_after_ms 由数据库时钟写入；用“DB 时钟锚点 + 单调 elapsed”换算本机时间域，
        // 避免新属主主机墙钟快于 DB 时提前获准写入（与旧属主租约重叠）。
        long dbNow = nowInDbMillis();
        if (dbNow < validAfter + validAfterSafetyMarginMs) {
            throw new StaleEpochException("owner not ready for " + cap + "/" + partition.value()
                    + " until " + validAfter);
        }
        LeaseValidator validator = leaseValidator.get();
        if (validator == null || !validator.isValid(cap, partition, primary.epoch(),
                clock.monotonicMillis())) {
            throw new StaleEpochException("ownership lease not valid for " + cap
                    + "/" + partition.value() + " epoch " + primary.epoch());
        }
    }

    /**
     * 当前 DB 时钟估计值：以最近一次成功刷新的 DB 时间为锚，叠加单调 elapsed。
     * 无 DB 锚点（内存实现/测试桩）时回退墙钟。
     */
    private long nowInDbMillis() {
        long anchorDb = dbAnchorDbMillis;
        long anchorMono = dbAnchorMonoMillis;
        if (anchorDb < 0 || anchorMono < 0) {
            return clock.nowMillis();
        }
        long elapsed = clock.monotonicMillis() - anchorMono;
        return anchorDb + Math.max(0L, elapsed);
    }

    /** 当前快照中由 self 持有的 PRIMARY 分区（租约续约输入）。 */
    public List<OwnedPartition> ownedPartitions(NodeId self) {
        Objects.requireNonNull(self, "self");
        Snapshot current = snapshot.get();
        List<OwnedPartition> owned = new ArrayList<>();
        for (Map.Entry<CapabilityType, Map<Integer, List<Placement>>> capEntry
                : current.byCapability().entrySet()) {
            for (Map.Entry<Integer, List<Placement>> partitionEntry : capEntry.getValue().entrySet()) {
                List<Placement> rows = partitionEntry.getValue();
                if (rows.isEmpty()) {
                    continue;
                }
                Placement primary = rows.getFirst();
                if (primary.isPrimary() && primary.node().equals(self)) {
                    owned.add(new OwnedPartition(capEntry.getKey(),
                            partitionEntry.getKey(), primary.epoch()));
                }
            }
        }
        return List.copyOf(owned);
    }

    /** 安装本地租约校验器；未安装时写路径 fail-closed。 */
    public void setLeaseValidator(LeaseValidator validator) {
        this.leaseValidator.set(validator);
    }

    /** 安装快照变更监听（用于放置变化后立即触发续约；回调必须非阻塞）。 */
    public void setSnapshotListener(Runnable listener) {
        this.snapshotListener = listener;
    }

    public long refreshCount() {
        return refreshCount.get();
    }

    public long reloadCount() {
        return reloadCount.get();
    }

    private void safeRefresh() {
        try {
            refreshNow();
        } catch (RuntimeException e) {
            long failures = consecutiveFailures.incrementAndGet();
            if (failures == 1 || failures % FAILURE_LOG_EVERY == 0) {
                LOG.warn("placement view refresh failed (keeping last snapshot, failures={}): {}",
                        failures, e.getMessage());
            }
        }
    }

    private void refresh() {
        Snapshot current = snapshot.get();
        long now = clock.nowMillis();
        boolean full = now - lastFullRefreshAt >= fullRefreshIntervalMs;
        Map<CapabilityType, Map<Integer, List<Placement>>> nextByCapability =
                new HashMap<>(current.byCapability());
        Map<CapabilityType, Long> nextRevisions = new HashMap<>(current.revisions());
        Set<CapabilityType> nextLoaded = EnumSet.noneOf(CapabilityType.class);
        nextLoaded.addAll(current.loaded());
        boolean placementsChanged = false;

        for (CapabilityType type : types) {
            long revision = placementStore.revision(type.name());
            boolean reload = full
                    || !current.loaded().contains(type)
                    || !Objects.equals(current.revisions().get(type), revision);
            if (!reload) {
                continue;
            }
            List<Placement> rows = placementStore.list(type.name());
            Map<Integer, List<Placement>> indexed = index(rows);
            Map<Integer, List<Placement>> previous = current.byCapability().get(type);
            if (!Objects.equals(previous, indexed)) {
                nextByCapability.put(type, indexed);
                placementsChanged = true;
            }
            nextRevisions.put(type, revision);
            nextLoaded.add(type);
            reloadCount.incrementAndGet();
        }

        if (full) {
            lastFullRefreshAt = now;
        }
        long dbNow = placementStore.dbNowMillis();
        if (dbNow > 0) {
            dbAnchorDbMillis = dbNow;
            dbAnchorMonoMillis = clock.monotonicMillis();
        }
        Snapshot next = new Snapshot(freeze(nextByCapability), Map.copyOf(nextRevisions),
                Set.copyOf(nextLoaded));
        snapshot.set(next);
        refreshCount.incrementAndGet();
        if (placementsChanged) {
            Runnable listener = snapshotListener;
            if (listener != null) {
                try {
                    listener.run();
                } catch (RuntimeException e) {
                    LOG.warn("placement snapshot listener failed: {}", e.toString());
                }
            }
        }
    }

    private static List<Placement> rows(Snapshot snapshot, CapabilityType cap, int partition) {
        Map<Integer, List<Placement>> byPartition = snapshot.byCapability().get(cap);
        if (byPartition == null) {
            return List.of();
        }
        List<Placement> rows = byPartition.get(partition);
        return rows == null ? List.of() : rows;
    }

    private static Map<Integer, List<Placement>> index(List<Placement> rows) {
        if (rows.isEmpty()) {
            return Map.of();
        }
        Map<Integer, List<Placement>> grouped = new LinkedHashMap<>();
        for (Placement row : rows) {
            grouped.computeIfAbsent(row.partition(), ignored -> new ArrayList<>()).add(row);
        }
        Map<Integer, List<Placement>> indexed = new LinkedHashMap<>();
        for (Map.Entry<Integer, List<Placement>> entry : grouped.entrySet()) {
            List<Placement> sorted = new ArrayList<>(entry.getValue());
            sorted.sort(Comparator.comparing(Placement::isPrimary).reversed()
                    .thenComparing(row -> row.node().value()));
            if (!sorted.getFirst().isPrimary()) {
                throw new IllegalStateException("partition " + entry.getKey()
                        + " has no PRIMARY placement row");
            }
            if (sorted.size() > 1 && sorted.get(1).isPrimary()) {
                throw new IllegalStateException("partition " + entry.getKey()
                        + " has duplicate PRIMARY placement rows");
            }
            indexed.put(entry.getKey(), List.copyOf(sorted));
        }
        return Map.copyOf(indexed);
    }

    private static Map<CapabilityType, Map<Integer, List<Placement>>> freeze(
            Map<CapabilityType, Map<Integer, List<Placement>>> byCapability) {
        Map<CapabilityType, Map<Integer, List<Placement>>> frozen =
                new EnumMap<>(CapabilityType.class);
        frozen.putAll(byCapability);
        return Map.copyOf(frozen);
    }

    /** 本地租约校验器（由 {@link PartitionLeaseManager} 实现）。 */
    @FunctionalInterface
    public interface LeaseValidator {
        boolean isValid(CapabilityType capability, PartitionId partition, long epoch, long nowMillis);
    }

    /** 本节点当前持有的分区（租约续约输入）。 */
    public record OwnedPartition(CapabilityType capability, int partition, long epoch) {
    }

    private record Snapshot(Map<CapabilityType, Map<Integer, List<Placement>>> byCapability,
                            Map<CapabilityType, Long> revisions,
                            Set<CapabilityType> loaded) {

        static Snapshot empty() {
            return new Snapshot(Map.of(), Map.of(), Set.of());
        }
    }
}
