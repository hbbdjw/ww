package com.ww.runtime.scheduler;

import com.ww.runtime.capability.InMemoryCapabilityStateStore;
import com.ww.runtime.cluster.InMemoryNodeRegistry;
import com.ww.runtime.common.CapabilityState;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.NodeInfo;
import com.ww.runtime.common.NodeState;
import com.ww.runtime.common.ResourceSnapshot;
import com.ww.runtime.common.SystemClock;
import com.ww.runtime.placement.InMemoryPlacementStore;
import com.ww.runtime.placement.PlacementEngine;
import com.ww.runtime.placement.RebalancePlanner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** 守护线程（start/stop）驱动的集成级测试：真实时间、短周期。 */
class ReconcileLoopLifecycleTest {

    private static final CapabilityType MOCK = CapabilityType.MOCK;
    private static final int PARTITIONS = 16;

    private final SystemClock clock = SystemClock.instance();
    private final InMemoryLeaseStore lease = new InMemoryLeaseStore(clock);
    private final InMemoryNodeRegistry registry = new InMemoryNodeRegistry();
    private final InMemoryCapabilityStateStore capabilities = new InMemoryCapabilityStateStore(clock);
    private final InMemoryPlacementStore placements = new InMemoryPlacementStore();
    private final PlacementEngine engine = new PlacementEngine();
    private final RecordingCommands commands = new RecordingCommands();
    private final CutoverMigrationCoordinator migration = new CutoverMigrationCoordinator(placements);

    private final List<ReconcileLoop> loops = new java.util.ArrayList<>();

    @AfterEach
    void tearDown() {
        loops.forEach(ReconcileLoop::stop);
    }

    private ReconcileLoop newLoop(NodeId self) {
        ReconcileLoop loop = new ReconcileLoop(self, lease, registry, capabilities, placements,
                engine, new RebalancePlanner(), commands, migration,
                SchedulerConfig.of(500L, 50L, 50L, PARTITIONS, 1, 0.10d, 100L),
                clock);
        loops.add(loop);
        return loop;
    }

    private void register(NodeId node) {
        registry.register(NodeInfo.of(node, "host-" + node.value(), 9000,
                NodeState.JOINING, 0L, 1.0d,
                ResourceSnapshot.of(1.0d, 100, 10, 100, 10, clock.nowMillis())));
    }

    @Test
    void twoLoopsElectExactlyOneLeader() {
        register(NodeId.of("X"));
        ReconcileLoop loopA = newLoop(NodeId.of("loop-A"));
        ReconcileLoop loopB = newLoop(NodeId.of("loop-B"));

        loopA.start();
        loopB.start();

        awaitUntil("exactly one leader elected", () -> loopA.isLeader() ^ loopB.isLeader());

        ReconcileLoop leader = loopA.isLeader() ? loopA : loopB;
        ReconcileLoop follower = loopA.isLeader() ? loopB : loopA;
        String expectedOwner = leader == loopA ? "loop-A" : "loop-B";
        assertThat(lease.currentOwner()).contains(expectedOwner);
        assertThat(follower.isLeader()).isFalse();
    }

    @Test
    void daemonLoopSurvivesRepeatedStoreFailuresAndStillConverges() {
        register(NodeId.of("X"));
        register(NodeId.of("Y"));
        DesiredStateWriter writer = new DesiredStateWriter(capabilities);
        writer.setDesired(NodeId.of("X"), MOCK, CapabilityState.ACTIVE);
        writer.setDesired(NodeId.of("Y"), MOCK, CapabilityState.ACTIVE);

        FlakyCapabilityStore flaky = new FlakyCapabilityStore(capabilities, 3);
        ReconcileLoop loop = new ReconcileLoop(NodeId.of("loop-A"), lease, registry, flaky,
                placements, engine, new RebalancePlanner(), commands, migration,
                SchedulerConfig.of(500L, 50L, 50L, PARTITIONS, 1, 0.10d, 100L),
                clock);
        loops.add(loop);

        loop.start();

        awaitUntil("loop survives store failures and bootstraps placement",
                () -> placements.primaries(MOCK.name()).size() == PARTITIONS);
        assertThat(loop.isLeader()).isTrue();
    }

    private static void awaitUntil(String description, java.util.function.BooleanSupplier condition) {
        long deadline = System.currentTimeMillis() + 5_000L;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(20L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while waiting: " + description, e);
            }
        }
        throw new AssertionError("condition not met within timeout: " + description);
    }
}
