package com.ww.runtime.membership;

import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.NodeInfo;
import com.ww.runtime.common.NodeState;
import com.ww.runtime.common.ResourceSnapshot;
import com.ww.runtime.common.SystemClock;
import com.ww.runtime.grpc.GrpcServerHost;
import com.ww.runtime.proto.MembershipEvent;
import com.ww.runtime.proto.MembershipGrpc;
import com.ww.runtime.proto.WatchReq;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 缺陷复现：{@code MembershipService.dispatch} 在 CopyOnWriteArrayList 的快照迭代器上
 * 调用 {@code it.remove()}——COW 迭代器不支持 remove，必抛 {@link UnsupportedOperationException}。
 *
 * <p>两条触发路径命中同一缺陷：</p>
 * <ul>
 *   <li>watcher 被取消（{@code isCancelled()==true}）→ try 块内 {@code it.remove()} 抛异常
 *       → 落入 catch → catch 内再次 {@code it.remove()} 二次抛异常并逃逸分发任务；</li>
 *   <li>{@code watcher.onNext} 抛 RuntimeException（传输层在 isCancelled 检查与 onNext 之间
 *       关闭流，或流已死但未标记取消）→ catch 内 {@code it.remove()} 抛异常逃逸。</li>
 * </ul>
 *
 * <p>后果：坏 watcher 永远无法从列表移除，每次分发都在它身上中断——
 * 列表中<b>其后注册</b>的全部 watcher 从此收不到任何事件（静默丢失）。</p>
 *
 * <p>真实触发路径是竞态（客户端取消与 dispatch 快照并发），本测试以确定性方式
 * 复现同一代码路径：向 watchers 列表注入一个 onNext 抛 StatusRuntimeException 的
 * 坏流观察者（位于真实 watcher 之前），然后验证后续 watcher 的事件送达。</p>
 */
class WatcherDispatchIteratorBugReproTest {

    private GrpcServerHost host;
    private MembershipService service;
    private ManagedChannel ch2;

    @AfterEach
    void tearDown() {
        if (service != null) {
            try {
                service.stop();
            } catch (RuntimeException ignored) {
                // teardown 尽力而为
            }
        }
        if (host != null) {
            host.stop();
        }
        if (ch2 != null) {
            ch2.shutdownNow();
        }
    }

    private static NodeInfo info(String id, int port) {
        return NodeInfo.of(NodeId.of(id), "localhost", port, NodeState.ACTIVE, 0L, 1.0,
                ResourceSnapshot.of(1.0, 1000, 100, 10_000, 1_000, 0L));
    }

    @Test
    void brokenWatcherBreaksEventDispatchForSubsequentWatchers() throws Exception {
        host = new GrpcServerHost();
        service = new MembershipService(NodeId.of("A"),
                () -> info("A", host.isRunning() ? host.getPort() : 9000),
                List::of,
                (nodeId, state) -> {
                },
                host,
                MembershipConfig.of(100L, 2, 4),
                SystemClock.instance());
        service.start();
        host.start(0);

        // 先注入坏 watcher 再注册真实 watcher，保证迭代顺序 [坏, watcher2]：
        // onNext 抛异常模拟流已死（真实场景为客户端取消与 dispatch 快照并发、
        // 或传输关闭夹在 isCancelled 检查与 onNext 之间）
        Field watchersField = MembershipService.class.getDeclaredField("watchers");
        watchersField.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<StreamObserver<MembershipEvent>> watchers =
                (List<StreamObserver<MembershipEvent>>) watchersField.get(service);
        watchers.add(new StreamObserver<>() {
            @Override
            public void onNext(MembershipEvent event) {
                throw new StatusRuntimeException(Status.CANCELLED);
            }

            @Override
            public void onError(Throwable t) {
            }

            @Override
            public void onCompleted() {
            }
        });

        // watcher2：真实 gRPC watcher，应持续收到事件
        ch2 = ManagedChannelBuilder.forAddress("localhost", host.getPort()).usePlaintext().build();
        List<MembershipEvent> received2 = new CopyOnWriteArrayList<>();
        CountDownLatch latch2 = new CountDownLatch(2);
        MembershipGrpc.newStub(ch2).watch(WatchReq.newBuilder().build(), new StreamObserver<>() {
            @Override
            public void onNext(MembershipEvent event) {
                if (event.getNode().getNodeId().equals("Y")
                        || event.getNode().getNodeId().equals("Z")) {
                    received2.add(event);
                    latch2.countDown();
                }
            }

            @Override
            public void onError(Throwable t) {
            }

            @Override
            public void onCompleted() {
            }
        });

        // 监听器先于 watchers 在同一分发任务中执行：证明 dispatch 任务确实逐个跑过
        AtomicInteger listenerEvents = new AtomicInteger();
        service.addListener(event -> listenerEvents.incrementAndGet());

        // 确认坏 watcher 位于列表头部、真实 watcher2 在其后（迭代顺序 = 列表顺序）
        // watch RPC 注册是异步的：轮询列表直到 watcher2 落入（不能用事件探测，
        // 因为坏 watcher 在列表头部会拦截一切事件——这本身就是待复现的缺陷）
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (watchers.size() < 2 && System.nanoTime() < deadline) {
            Thread.sleep(20L);
        }
        assertThat(watchers).hasSizeGreaterThan(1);

        // 连续两个事件：dispatch 遍历到坏 watcher → onNext 抛异常 → catch 内
        // it.remove() 抛 UnsupportedOperationException 逃逸分发任务 → watcher2 静默丢失
        service.merge(info("Y", 9002), false);
        Thread.sleep(300L);
        service.merge(info("Z", 9003), false);

        boolean receivedBoth = latch2.await(3, TimeUnit.SECONDS);
        Thread.sleep(200L); // 给分发任务收尾的时间

        // 诊断锚点：事件在分发任务侧确实全部产生（监听器不受 watcher 循环异常影响）
        assertThat(listenerEvents.get())
                .as("监听器收到全部事件，证明 dispatch 任务均已执行")
                .isEqualTo(2);
        assertThat(receivedBoth)
                .as("坏 watcher 不应阻断 watcher2 的事件分发；实际 dispatch 在 "
                        + "CopyOnWriteArrayList 快照迭代器上调用 remove() 抛 UnsupportedOperationException，"
                        + "分发任务中断、坏 watcher 永远无法移除，其后事件对 watcher2 持续静默丢失；"
                        + "watcher2 实际收到 %d/2 个事件", received2.size())
                .isTrue();
        assertThat(received2.stream().filter(e -> e.getNode().getNodeId().equals("Y")).count())
                .as("事件 Y 应送达 watcher2").isEqualTo(1);
        assertThat(received2.stream().filter(e -> e.getNode().getNodeId().equals("Z")).count())
                .as("事件 Z 应送达 watcher2").isEqualTo(1);
    }
}
