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
 * 深审缺陷 6（回归重建，§6.1）：{@code NodeRuntime.start()} 启动失败不得残留 JOINING 孤儿。
 *
 * <p>启动序列先 {@code registry.register(JOINING)} 再 {@code grpcHost.start(port)}。
 * 原缺陷：端口占用等绑定失败抛 IOException 后注册表残留 JOINING 记录，
 * {@code listAlive()} 将其视为可用节点（写命令石沉大海、治愈永无休止），
 * 且 {@code shutdown()} 在 {@code started=false} 时不清理。</p>
 *
 * <p>修复（§3.12④ 深审6内联回滚）：start 失败路径内联 {@code tombstone(self)} +
 * 回收本路径启动的宿主；shutdown 仍无条件清理作二次兜底。
 * 本测试断言修复后行为：start 失败后立即 DEAD、不在 listAlive。</p>
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
    void startFailureTombstonesInlineNoJoiningOrphan() throws IOException {
        InMemoryNodeRegistry registry = new InMemoryNodeRegistry();

        // 占用一个真实端口，制造第二个 host 的绑定失败
        blocker = new GrpcServerHost();
        blocker.start(0);
        int occupiedPort = blocker.getPort();

        failingHost = new GrpcServerHost();
        NodeRuntime runtime = new NodeRuntime(SELF, "localhost", occupiedPort, 8080, 1.0d,
                registry, failingHost, () -> snapshot(), SystemClock.instance(), 1_000L);

        // 启动失败（端口占用）：异常照常上抛
        assertThatThrownBy(runtime::start).isInstanceOf(IOException.class);

        // 修复断言：注册行内联 tombstone 为 DEAD，不残留 JOINING、不进伸缩视图
        assertThat(registry.get(SELF)).isPresent();
        assertThat(registry.get(SELF).orElseThrow().state())
                .as("start 失败必须内联回滚为 DEAD（不留 JOINING 孤儿）")
                .isEqualTo(NodeState.DEAD);
        assertThat(registry.listAlive()).extracting(info -> info.id())
                .as("孤儿不得进入 listAlive 污染伸缩视图").doesNotContain(SELF);

        // shutdown 二次调用幂等，且不复活记录
        runtime.shutdown();
        assertThat(registry.get(SELF).orElseThrow().state()).isEqualTo(NodeState.DEAD);
    }
}
