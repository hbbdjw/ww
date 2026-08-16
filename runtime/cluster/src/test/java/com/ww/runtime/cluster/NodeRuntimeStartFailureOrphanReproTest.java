package com.ww.runtime.cluster;

import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.NodeState;
import com.ww.runtime.common.ResourceSnapshot;
import com.ww.runtime.common.SystemClock;
import com.ww.runtime.grpc.GrpcServerHost;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 缺陷复现：{@code NodeRuntime.start()} 启动失败时残留 JOINING 孤儿记录，
 * 且 {@code shutdown()} 因 {@code started=false} 拒绝执行任何清理。
 *
 * <p>启动序列先 {@code registry.register(JOINING)} 再 {@code grpcHost.start(port)}；
 * 端口占用等绑定失败抛 IOException 后：</p>
 * <ol>
 *   <li>注册表中遗留 JOINING 行——心跳永不到来、无人推进状态；
 *       {@code listAlive()} 视其为存活节点，调度器会向它下发命令并等待一个
 *       永远不会就绪的节点（放置环可能把它算进去）；</li>
 *   <li>{@code started} 未置位 → {@code shutdown()} 提前返回，不做 tombstone，
 *       孤儿记录无法通过正常停机路径清理，只能人工改库或等外部故障检测。</li>
 * </ol>
 */
class NodeRuntimeStartFailureOrphanReproTest {

    private static final NodeId SELF = NodeId.of("node-x");

    private GrpcServerHost blocker;
    private GrpcServerHost failingHost;

    @AfterEach
    void tearDown() {
        if (blocker != null) {
            blocker.stop();
        }
        if (failingHost != null) {
            failingHost.stop();
        }
    }

    private static ResourceSnapshot snapshot() {
        return ResourceSnapshot.of(1.0d, 100, 10, 100, 10, 0L);
    }

    @Test
    void startFailureLeavesJoiningOrphanThatShutdownCannotClean() throws IOException {
        InMemoryNodeRegistry registry = new InMemoryNodeRegistry();

        // 占用一个真实端口，制造第二个 host 的绑定失败
        blocker = new GrpcServerHost();
        blocker.start(0);
        int occupiedPort = blocker.getPort();

        failingHost = new GrpcServerHost();
        NodeRuntime runtime = new NodeRuntime(SELF, "localhost", occupiedPort, 1.0d,
                registry, failingHost, () -> snapshot(), SystemClock.instance(), 1_000L);

        // 启动失败（端口占用）：register(JOINING) 已执行，host.start 抛 IOException
        assertThatThrownBy(runtime::start).isInstanceOf(IOException.class);

        // 孤儿记录：停留在 JOINING
        assertThat(registry.get(SELF)).isPresent();
        assertThat(registry.get(SELF).orElseThrow().state()).isEqualTo(NodeState.JOINING);
        assertThat(registry.listAlive()).extracting(info -> info.id())
                .as("JOINING 孤儿被 listAlive 视为可用节点").contains(SELF);

        // 期望：启动失败路径或 shutdown() 应 tombstone 清理；
        // 实际：shutdown 因 started=false 直接返回，JOINING 永久残留。
        runtime.shutdown();
        assertThat(registry.get(SELF).orElseThrow().state())
                .as("start 失败后 shutdown 未做任何清理：应 tombstone 为 DEAD，实际仍为 JOINING")
                .isEqualTo(NodeState.DEAD);
    }
}
