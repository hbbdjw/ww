package com.ww.runtime.scheduler;

import com.ww.runtime.capability.InMemoryCapabilityStateStore;
import com.ww.runtime.cluster.InMemoryNodeRegistry;
import com.ww.runtime.cluster.InMemoryNodeSuspicionStore;
import com.ww.runtime.cluster.NodeSuspicionStore;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.NodeInfo;
import com.ww.runtime.common.NodeState;
import com.ww.runtime.common.ResourceSnapshot;
import com.ww.runtime.common.TestClock;
import com.ww.runtime.placement.InMemoryPlacementStore;
import com.ww.runtime.placement.PlacementEngine;
import com.ww.runtime.placement.RebalancePlanner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 多级故障确认仲裁：leader 按 TTL 内不同观察者怀疑票的法定人数
 * 唯一执行 ACTIVE/SUSPECTED → DEAD 提升（PFAIL→FAIL）。
 */
class SuspicionQuorumTest {

    private static final int PARTITIONS = 16;
    private static final NodeId SCHEDULER = NodeId.of("scheduler-node");
    private static final NodeId A = NodeId.of("A");
    private static final NodeId B = NodeId.of("B");
    private static final NodeId C = NodeId.of("C");
    private static final NodeId D = NodeId.of("D");

    private TestClock clock;
    private InMemoryLeaseStore lease;
    private InMemoryNodeRegistry registry;
    private InMemoryCapabilityStateStore capabilities;
    private InMemoryPlacementStore placements;
    private InMemoryNodeSuspicionStore suspicions;

    @BeforeEach
    void setUp() {
        clock = new TestClock(1_000L);
        lease = new InMemoryLeaseStore(clock);
        registry = new InMemoryNodeRegistry();
        capabilities = new InMemoryCapabilityStateStore(clock);
        placements = new InMemoryPlacementStore();
        suspicions = new InMemoryNodeSuspicionStore(clock);
        for (NodeId node : List.of(A, B, C, D)) {
            registry.register(info(node));
            registry.markState(node, NodeState.ACTIVE);
        }
    }

    private NodeInfo info(NodeId node) {
        return NodeInfo.of(node, "host-" + node.value(), 9000,
                NodeState.JOINING, 0L, 1.0d,
                ResourceSnapshot.of(1.0d, 100, 10, 100, 10, clock.nowMillis()));
    }

    /** 仲裁开启配置：票 TTL 12s，最小票数 1，3 节点法定 2 票、2 节点法定 1 票。 */
    private SchedulerConfig quorumConfig() {
        return new SchedulerConfig(10_000L, 3_000L, 2_000L, PARTITIONS, 1, 1, 0.10d,
                30_000L, 30_000L, 12_000L, 1, true);
    }

    private ReconcileLoop loop(SchedulerConfig config, NodeSuspicionStore store) {
        return new ReconcileLoop(SCHEDULER, lease, registry, capabilities, placements,
                new PlacementEngine(), new RebalancePlanner(), new RecordingCommands(),
                new NoopMigrationCoordinator(), config, clock, store);
    }

    @Test
    void majorityVotesPromoteSuspectToDeadAndClearVotes() {
        suspicions.vote(B, A, 8.5);
        suspicions.vote(B, C, 9.0);

        ReconcileLoop loop = loop(quorumConfig(), suspicions);
        assertThat(loop.runOnce()).isTrue();

        assertThat(registry.get(B)).get().extracting(NodeInfo::state).isEqualTo(NodeState.DEAD);
        // 判死后票清，下周期不再重复处理；A/C/D 存活（SCHEDULER 未注册为数据节点）
        assertThat(suspicions.freshVoteCount(B, clock.nowMillis(), 12_000L)).isZero();
        assertThat(registry.listAlive()).extracting(NodeInfo::id)
                .containsExactlyInAnyOrder(A, C, D)
                .doesNotContain(B);
    }

    @Test
    void singleVoteInThreeNodeClusterDoesNotPromote() {
        suspicions.vote(B, A, 9.0);
        loop(quorumConfig(), suspicions).runOnce();
        assertThat(registry.get(B)).get().extracting(NodeInfo::state).isEqualTo(NodeState.ACTIVE);
    }

    @Test
    void expiredVotesDoNotCount() {
        suspicions.vote(B, A, 9.0);
        suspicions.vote(B, C, 9.0);
        clock.advance(13_000L); // 超过票 TTL
        loop(quorumConfig(), suspicions).runOnce();
        assertThat(registry.get(B)).get().extracting(NodeInfo::state).isEqualTo(NodeState.ACTIVE);
    }

    @Test
    void twoNodeClusterRequiresOnlyOneVote() {
        registry.tombstone(C);
        registry.tombstone(D);
        suspicions.vote(B, A, 9.0); // 存活仅 A、B（SCHEDULER 未注册）→ 法定 1 票
        loop(quorumConfig(), suspicions).runOnce();
        assertThat(registry.get(B)).get().extracting(NodeInfo::state).isEqualTo(NodeState.DEAD);
    }

    @Test
    void quorumDisabledNeverPromotesEvenWithUnanimousVotes() {
        SchedulerConfig disabled = new SchedulerConfig(10_000L, 3_000L, 2_000L, PARTITIONS, 1, 1,
                0.10d, 30_000L, 30_000L, 12_000L, 1, false);
        suspicions.vote(B, A, 9.0);
        suspicions.vote(B, C, 9.0);
        suspicions.vote(B, D, 9.0);
        loop(disabled, suspicions).runOnce();
        assertThat(registry.get(B)).get().extracting(NodeInfo::state).isEqualTo(NodeState.ACTIVE);
    }

    @Test
    void promotedDeadRowIsIdempotentAndVotesCleared() {
        registry.markState(B, NodeState.SUSPECTED);
        suspicions.vote(B, A, 9.0);
        suspicions.vote(B, C, 9.0);
        ReconcileLoop loop = loop(quorumConfig(), suspicions);
        loop.runOnce();
        assertThat(registry.get(B)).get().extracting(NodeInfo::state).isEqualTo(NodeState.DEAD);
        // 再来一票 + 再跑周期：保持 DEAD，不抛异常
        suspicions.vote(B, A, 9.0);
        loop.runOnce();
        assertThat(registry.get(B)).get().extracting(NodeInfo::state).isEqualTo(NodeState.DEAD);
    }
}
