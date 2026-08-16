package com.ww.runtime.capability;

import com.ww.runtime.common.CapabilityState;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.TestClock;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 缺陷复现：{@code CapabilityManager.transition} 的「读状态 → 校验合法 → 写状态」
 * 是非原子的 check-then-act，且类上无任何同步。
 *
 * <p>命令来源天然多线程：ClusterControlServer 运行在 gRPC 线程池上（调度器重试、
 * 双 leader 重叠窗口、人工运维命令可并发到达同一节点）。两个并发 start 可同时读到
 * DISABLED、同时通过合法性校验、双双写入 STARTING 并<b>重复执行模块钩子</b>
 * （start()/warm() 各两次）——重量级模块（建连接、回放日志）会被重复初始化。</p>
 *
 * <p>本测试用门闩仓库把两个线程同时钉在「读状态」一刻，确定性复现交织。</p>
 */
class CapabilityManagerTransitionRaceReproTest {

    private static final NodeId NODE = NodeId.of("node-x");
    private static final CapabilityType MOCK = CapabilityType.MOCK;

    /** 计数钩子模块：统计 start/warm 实际执行次数。 */
    private static final class CountingModule implements CapabilityModule {
        final AtomicInteger starts = new AtomicInteger();
        final AtomicInteger warms = new AtomicInteger();

        @Override
        public CapabilityType type() {
            return MOCK;
        }

        @Override
        public void start() {
            starts.incrementAndGet();
        }

        @Override
        public void warm() {
            warms.incrementAndGet();
        }

        @Override
        public void drain() {
        }

        @Override
        public void stop() {
        }

        @Override
        public void freeze(boolean frozen) {
        }
    }

    /** armed 后的前两次 get() 在屏障处会合：保证两个线程同时读到 DISABLED 后才各自写 STARTING。 */
    private static final class GatedStore implements CapabilityStateStore {
        private final InMemoryCapabilityStateStore delegate;
        private final CyclicBarrier gate;
        private final AtomicInteger gateBudget = new AtomicInteger(2);
        private volatile boolean armed;

        GatedStore(InMemoryCapabilityStateStore delegate, CyclicBarrier gate) {
            this.delegate = delegate;
            this.gate = gate;
        }

        @Override
        public Optional<CapabilityRecord> get(NodeId node, CapabilityType type) {
            if (armed && gateBudget.decrementAndGet() >= 0) {
                try {
                    gate.await(5, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
            return delegate.get(node, type);
        }

        @Override
        public void setDesired(NodeId node, CapabilityType type, CapabilityState desired) {
            delegate.setDesired(node, type, desired);
        }

        @Override
        public void setActual(NodeId node, CapabilityType type, CapabilityState actual) {
            delegate.setActual(node, type, actual);
        }

        @Override
        public List<CapabilityRecord> list() {
            return delegate.list();
        }
    }

    @Test
    void concurrentStartExecutesModuleHooksTwice() throws Exception {
        TestClock clock = new TestClock(1_000L);
        GatedStore store = new GatedStore(new InMemoryCapabilityStateStore(clock),
                new CyclicBarrier(2));
        CapabilityManager manager = new CapabilityManager(NODE, store, clock);
        CountingModule module = new CountingModule();
        manager.register(module);
        store.armed = true; // 注册完成后武装门闩，仅拦截竞态双方的状态读取

        Thread t1 = new Thread(() -> manager.start(MOCK), "racer-1");
        Thread t2 = new Thread(() -> manager.start(MOCK), "racer-2");
        t1.start();
        t2.start();
        t1.join(10_000L);
        t2.join(10_000L);

        assertThat(manager.state(MOCK)).isEqualTo(CapabilityState.ACTIVE);
        assertThat(module.starts.get())
                .as("transition 为非原子 check-then-act：两线程同读 DISABLED 后双双通过校验，"
                        + "模块 start() 被重复执行（应为 1 次）")
                .isEqualTo(1);
        assertThat(module.warms.get())
                .as("warm() 同样被重复执行（应为 1 次）")
                .isEqualTo(1);
    }
}
