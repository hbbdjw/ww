package com.ww.runtime.membership;

import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.NodeInfo;
import com.ww.runtime.common.NodeState;
import com.ww.runtime.common.ResourceSnapshot;
import com.ww.runtime.common.TestClock;
import com.ww.runtime.grpc.GrpcServerHost;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * φ 检测器接入 {@link MembershipService} 的确定性测试：
 * TestClock 驱动包内可见的信号记录与故障检测，不经过网络。
 */
@DisplayName("Phi Accrual 接入：暖窗回退 / 自适应怀疑 / 连续确认判死 / 恢复")
class PhiDetectionMembershipTest {

    private static final NodeId SELF = NodeId.of("self");
    private static final NodeId B = NodeId.of("B");

    private final TestClock clock = new TestClock(1_000_000L);
    private GrpcServerHost host;
    private MembershipService service;
    private final List<NodeState> sinkCalls = new CopyOnWriteArrayList<>();
    private final List<NodeId> votes = new CopyOnWriteArrayList<>();
    private final List<NodeId> recalls = new CopyOnWriteArrayList<>();

    private static NodeInfo info(NodeId id, NodeState state) {
        return NodeInfo.of(id, "h-" + id.value(), 9000, state, 0L, 1.0,
                ResourceSnapshot.of(1.0, 100, 10, 100, 10, 0L));
    }

    private void start(MembershipConfig config) {
        start(config, SuspicionSink.NOOP);
    }

    private void start(MembershipConfig config, SuspicionSink suspicionSink) {
        host = new GrpcServerHost();
        service = new MembershipService(SELF,
                () -> info(SELF, NodeState.ACTIVE),
                () -> List.of(),
                (id, state) -> sinkCalls.add(state),
                host, config, clock, suspicionSink);
        service.start();
        service.merge(info(B, NodeState.ACTIVE), true);
        // 排空 start() 初始延迟=0 的首个真实 tick（此时 B 无 lastHeard，检测跳过）；
        // 后续 tick 在 10s 真实时间之后，远晚于测试体，避免与确定性驱动相互叠加。
        try {
            Thread.sleep(300L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    @AfterEach
    void stop() {
        if (service != null) {
            service.stop();
        }
        if (host != null) {
            host.stop();
        }
    }

    /** 喂入 n 次严格 1000ms 间隔的存活信号。 */
    private void beat(int times) {
        for (int i = 0; i < times; i++) {
            clock.advance(1_000L);
            service.recordSignal(B, clock.nowMillis());
        }
    }

    private NodeState stateOfB() {
        return service.view().get(B).state();
    }

    @Test
    @DisplayName("暖窗期样本不足：回退固定超时判定 SUSPECTED→DEAD")
    void warmWindowFallsBackToFixedTimeouts() {
        // interval=10_000 避开真实调度 tick；φ 默认参数
        start(MembershipConfig.of(10_000L, 3, 6));
        service.recordSignal(B, clock.nowMillis()); // 仅首个时间戳，零间隔样本 → 未就绪

        clock.advance(30_001L); // > suspectTimeout(30s)
        service.detectFailures(clock.nowMillis());
        assertThat(stateOfB()).isEqualTo(NodeState.SUSPECTED);

        clock.advance(30_000L); // > deadTimeout(60s)
        service.detectFailures(clock.nowMillis());
        assertThat(stateOfB()).isEqualTo(NodeState.DEAD);
    }

    @Test
    @DisplayName("稳态：健康节拍 φ≈0；约 3σ 静默才 SUSPECTED，且 φ 可观测")
    void steadyRhythmHealthyThenSuspectedAroundThreeSigma() {
        start(MembershipConfig.of(10_000L, 3, 6));
        beat(10);
        assertThat(service.phiOf(B)).isLessThan(0.05);

        // 1 个均值 + 3σ = 1300ms：φ≈2.87 < 3，仍健康（固定超时下 1.3s 早已误判）
        clock.advance(1_300L);
        service.detectFailures(clock.nowMillis());
        assertThat(stateOfB()).isEqualTo(NodeState.ACTIVE);

        // 1 个均值 + 4σ = 1400ms：y=4，φ≈4.75 ≥ 3 → SUSPECTED
        clock.advance(100L);
        service.detectFailures(clock.nowMillis());
        assertThat(stateOfB()).isEqualTo(NodeState.SUSPECTED);
        assertThat(service.phiOf(B)).isGreaterThanOrEqualTo(3.0);
    }

    @Test
    @DisplayName("DEAD 需连续 2 个检测周期 φ≥死阈值：首周期只到 SUSPECTED")
    void deadRequiresTwoConsecutiveOverThresholdTicks() {
        start(MembershipConfig.of(10_000L, 3, 6)); // deadConfirmations 默认 2
        beat(10);

        // 深度静默（y≈20，φ 远超 8）
        clock.advance(3_000L);
        service.detectFailures(clock.nowMillis());
        assertThat(stateOfB())
                .as("首个超死阈值周期只升级 SUSPECTED，保证事件先疑似后死亡")
                .isEqualTo(NodeState.SUSPECTED);

        // 下一周期仍超死阈值 → DEAD
        service.detectFailures(clock.nowMillis());
        assertThat(stateOfB()).isEqualTo(NodeState.DEAD);
    }

    @Test
    @DisplayName("仅一个周期超死阈值后心跳恢复：不判 DEAD 并回到 ACTIVE")
    void singleOverDeadTickThenHeartbeatRecovers() {
        start(MembershipConfig.of(10_000L, 3, 6));
        beat(10);

        clock.advance(3_000L);
        service.detectFailures(clock.nowMillis());
        assertThat(stateOfB()).isEqualTo(NodeState.SUSPECTED);

        // 心跳恢复：连续计数清零，SUSPECTED → ACTIVE
        clock.advance(0L);
        service.recordSignal(B, clock.nowMillis());
        service.detectFailures(clock.nowMillis());
        assertThat(stateOfB()).isEqualTo(NodeState.ACTIVE);
        assertThat(service.phiOf(B)).isLessThan(0.05);
    }

    @Test
    @DisplayName("acceptablePause 宽限内的静默不产生怀疑")
    void acceptablePauseSuppressesSuspicion() {
        // pause=2000，其余默认
        start(MembershipConfig.of(10_000L, 3, 6, 3.0, 8.0, 2, 200, 3, 100L, 2_000L));
        beat(10);
        clock.advance(2_900L); // 扣除 2000 pause，有效静默 900（y=-1）→ 健康
        service.detectFailures(clock.nowMillis());
        assertThat(stateOfB()).isEqualTo(NodeState.ACTIVE);
    }

    private SuspicionSink recordingSuspicionSink() {
        return new SuspicionSink() {
            @Override
            public void vote(NodeId suspect, double phi) {
                votes.add(suspect);
            }

            @Override
            public void recall(NodeId suspect) {
                recalls.add(suspect);
            }
        };
    }

    @Test
    @DisplayName("多级仲裁：SUSPECTED 期间投票，恢复 ACTIVE 撤票")
    void votesWhileSuspiciousAndRecallsOnRecovery() {
        start(MembershipConfig.of(10_000L, 3, 6), recordingSuspicionSink());
        beat(10);

        clock.advance(1_400L); // φ≈4.75 → SUSPECTED
        service.detectFailures(clock.nowMillis());
        assertThat(stateOfB()).isEqualTo(NodeState.SUSPECTED);
        assertThat(votes).contains(B);

        // 持续疑似：每个检测周期刷新投票（TTL 内保持新鲜）
        service.detectFailures(clock.nowMillis());
        assertThat(votes.stream().filter(B::equals).count()).isGreaterThanOrEqualTo(2);

        // 心跳恢复 → SUSPECTED 转 ACTIVE，撤票
        service.recordSignal(B, clock.nowMillis());
        service.detectFailures(clock.nowMillis());
        assertThat(stateOfB()).isEqualTo(NodeState.ACTIVE);
        assertThat(recalls).contains(B);
    }

    @Test
    @DisplayName("仲裁模式：本地判 DEAD 不回写注册表（仅 SUSPECTED 回写），但投票不停；stop 撤票")
    void quorumModeSuppressesLocalDeadWritebackButKeepsVoting() {
        start(MembershipConfig.of(10_000L, 3, 6, 3.0, 8.0, 2, 200, 3, 100L, 0L, false),
                recordingSuspicionSink());
        beat(10);

        clock.advance(3_000L);
        service.detectFailures(clock.nowMillis()); // SUSPECTED
        service.detectFailures(clock.nowMillis()); // DEAD（本地视图）
        assertThat(stateOfB()).isEqualTo(NodeState.DEAD);

        // SUSPECTED 回写保留（LB 摘流/可见性），DEAD 回写被抑制（只能由 leader 仲裁提升）
        assertThat(sinkCalls).contains(NodeState.SUSPECTED);
        assertThat(sinkCalls).doesNotContain(NodeState.DEAD);
        assertThat(votes.stream().filter(B::equals).count()).isGreaterThanOrEqualTo(2);

        service.stop(); // 优雅退出撤掉本节点投出的未决票
        assertThat(recalls).contains(B);
    }
}
