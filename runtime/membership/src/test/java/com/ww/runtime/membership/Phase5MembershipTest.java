package com.ww.runtime.membership;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.NodeInfo;
import com.ww.runtime.common.NodeState;
import com.ww.runtime.common.ResourceSnapshot;
import com.ww.runtime.common.SystemClock;
import com.ww.runtime.grpc.GrpcServerHost;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 阶段 5 验收（成员子系统的视图语义，§2.2 S10/S12 + §3.11 S1②）：
 * <ul>
 *   <li>S10：资源波动不驱动事件、不推进视图版本；实质变更事件按窗口节流；</li>
 *   <li>S12：状态回写携带判定纪元与直证标记（宿主防护/豁免的依据）；</li>
 *   <li>S1②：视图版本号仅在实质变化时自增（ack 空成员协商的正确性前提）。</li>
 * </ul>
 *
 * <p>不经网络：直接以包内可见的 {@code merge}/{@code transitionLocal 同构入口}驱动视图，
 * 心跳目录置空使守护 tick 无副作用。</p>
 */
@DisplayName("阶段 5 验收：视图事件/版本/S12 回写标记")
class Phase5MembershipTest {

    private static final NodeId A = NodeId.of("A");
    private static final NodeId B = NodeId.of("B");
    private static final NodeId SELF = NodeId.of("self");

    /** 四参回写记录。 */
    private record SinkCall(NodeId id, NodeState state, long observedEpoch, boolean directEvidence) {
    }

    private final List<SinkCall> sinkCalls = new CopyOnWriteArrayList<>();
    private final List<MemberEvent> events = new CopyOnWriteArrayList<>();
    private final CountDownLatch eventGate = new CountDownLatch(1);

    private GrpcServerHost host;
    private MembershipService service;

    private static NodeInfo info(NodeId id, NodeState state, long epoch, double weight, long resAt) {
        return NodeInfo.of(id, "h-" + id.value(), 9000, state, epoch, weight,
                ResourceSnapshot.of(1.0d, 100, 10, 100, 10, resAt));
    }

    @BeforeEach
    void start() {
        host = new GrpcServerHost();
        StateSink sink = new StateSink() {
            @Override
            public void apply(NodeId id, NodeState newState) {
                apply(id, newState, -1L, false);
            }

            @Override
            public void apply(NodeId id, NodeState newState, long observedEpoch, boolean directEvidence) {
                sinkCalls.add(new SinkCall(id, newState, observedEpoch, directEvidence));
            }
        };
        service = new MembershipService(SELF,
                () -> info(SELF, NodeState.ACTIVE, 0L, 1.0d, 0L),
                () -> List.of(), // 目录恒空：tick 无发送
                sink, host,
                MembershipConfig.of(10_000L, 50, 90), // 超长间隔：测试期内不触发检测 tick
                SystemClock.instance());
        service.addListener(event -> {
            events.add(event);
            eventGate.countDown();
        });
        service.start();
    }

    @AfterEach
    void stop() {
        service.stop();
        host.stop();
    }

    private void awaitEvents(int min) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 2_000;
        while (events.size() < min && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
    }

    @Test
    @DisplayName("S10/S1②：资源波动仅更新视图——无事件、版本不推进")
    void resourceOnlyChangeProducesNoEventAndNoVersionBump() throws Exception {
        service.merge(info(A, NodeState.ACTIVE, 3L, 1.0d, 100L), true);
        awaitEvents(1);
        long versionAfterAdd = service.viewVersion();
        int eventsAfterAdd = events.size();

        // 同 epoch/state/权重，仅资源时间戳变化，直证来源 → 视图吸收但无事件
        service.merge(info(A, NodeState.ACTIVE, 3L, 1.0d, 999L), true);

        assertThat(service.view().get(A).resources().updatedAtMillis())
                .as("视图资源应被更新").isEqualTo(999L);
        Thread.sleep(100);
        assertThat(events).as("资源波动不得驱动事件").hasSize(eventsAfterAdd);
        assertThat(service.viewVersion()).as("资源波动不推进视图版本").isEqualTo(versionAfterAdd);
    }

    @Test
    @DisplayName("S10：实质变更（权重）产生 NODE_UPDATED 且按窗口节流——第二次实质变更被吞")
    void materialChangeEmitsOnceThenThrottled() throws Exception {
        service.merge(info(A, NodeState.ACTIVE, 1L, 1.0d, 100L), true);
        awaitEvents(1);
        int before = events.size();

        service.merge(info(A, NodeState.ACTIVE, 1L, 2.5d, 100L), true); // 权重变化
        awaitEvents(before + 1);
        assertThat(events).anySatisfy(e -> {
            assertThat(e.type()).isEqualTo(MemberEvent.NODE_UPDATED);
            assertThat(e.node().weight()).isEqualTo(2.5d);
        });

        long versionAfterFirst = service.viewVersion();
        service.merge(info(A, NodeState.ACTIVE, 1L, 4.0d, 100L), true); // 再次实质变更：节流窗口内
        Thread.sleep(100);
        assertThat(events.stream().filter(e -> e.type().equals(MemberEvent.NODE_UPDATED)).count())
                .as("NODE_UPDATED 节流窗口内至多一次").isEqualTo(1);
        assertThat(service.viewVersion()).as("版本仍推进（节流只限事件，不限 ack 协商）")
                .isGreaterThan(versionAfterFirst);
    }

    @Test
    @DisplayName("S12：SUSPECTED→ACTIVE 恢复回写携带判定纪元与直证标记（宿主据此防护/豁免）")
    void recoveredWriteCarriesEpochAndDirectFlag() throws Exception {
        // 他报把 A 判 SUSPECTED（较高 epoch）
        service.merge(info(A, NodeState.SUSPECTED, 5L, 1.0d, 100L), false);
        awaitEvents(1);
        sinkCalls.clear();

        // 他报恢复 ACTIVE（epoch 6，非直证）→ 回写 observedEpoch=6、direct=false
        service.merge(info(A, NodeState.ACTIVE, 6L, 1.0d, 100L), false);
        awaitRecovery();
        assertThat(sinkCalls).anySatisfy(call -> {
            assertThat(call.id()).isEqualTo(A);
            assertThat(call.state()).isEqualTo(NodeState.ACTIVE);
            assertThat(call.observedEpoch()).isEqualTo(6L);
            assertThat(call.directEvidence()).isFalse();
        });

        // 直证自报（B 从 SUSPECTED 回归）→ direct=true 豁免位必须为 true
        service.merge(info(B, NodeState.SUSPECTED, 2L, 1.0d, 100L), false);
        sinkCalls.clear();
        service.merge(info(B, NodeState.ACTIVE, 0L, 1.0d, 100L), true); // B 重启后 epoch 重置的自报
        awaitRecovery();
        assertThat(sinkCalls).anySatisfy(call -> {
            assertThat(call.id()).isEqualTo(B);
            assertThat(call.directEvidence()).as("直证自报必须携带豁免标记").isTrue();
        });
    }

    private void awaitRecovery() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 2_000;
        while (sinkCalls.isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
    }

    @Test
    @DisplayName("S1②：新节点并入即推进版本")
    void newMemberBumpsVersion() {
        long before = service.viewVersion();
        service.merge(info(A, NodeState.ACTIVE, 1L, 1.0d, 100L), true);
        assertThat(service.viewVersion()).isGreaterThan(before);
        assertThat(service.view()).containsKey(A);
    }
}
