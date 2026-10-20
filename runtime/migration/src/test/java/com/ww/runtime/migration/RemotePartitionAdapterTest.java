package com.ww.runtime.migration;

import com.ww.runtime.capability.MockCapability;
import com.ww.runtime.capability.PlacementView;
import com.ww.runtime.common.MigrationTaskState;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.TestClock;
import com.ww.runtime.grpc.GrpcServerHost;
import com.ww.runtime.placement.Partitioner;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * RemotePartitionAdapter 对真实 gRPC 服务端的客户端行为：
 * 接收端任务已终态时导入重发耗尽返回 false（迟到的重复执行者场景），
 * 远端清理失败经 ack 透传 false（拒绝原因随 ack message 进客户端日志）。
 */
class RemotePartitionAdapterTest {

    private static final int PARTITION_COUNT = 4096;
    private static final NodeId NODE_A = NodeId.of("node-a");
    private static final NodeId NODE_B = NodeId.of("node-b");

    private final MockCapability capability = new MockCapability(
            NODE_A, emptyView(), key -> Partitioner.partition(key, PARTITION_COUNT));
    private final InMemoryMigrationTaskStore store = new InMemoryMigrationTaskStore();
    private GrpcServerHost host;

    private static PlacementView emptyView() {
        return (cap, partition) -> Optional.empty();
    }

    private static int partitionOf(String seed) {
        return Partitioner.partition(seed, PARTITION_COUNT);
    }

    @BeforeEach
    void setUp() throws Exception {
        host = new GrpcServerHost();
        host.addService(new MigrationServer(new MockPartitionAdapter(capability), store,
                new TestClock()));
        host.start(0);
    }

    @AfterEach
    void tearDown() throws Exception {
        host.stop();
        host.awaitTermination(5, TimeUnit.SECONDS);
    }

    @Test
    void importChunksReturnsFalseWhenReceiverTaskAlreadyTerminal() {
        // 重复执行者场景：并发执行者已把任务推进到 COMPLETED，对端 TransferStream 恒拒
        // （ok=false + "already terminal"）——重发耗尽后返回 false，且数据不得落盘
        int partition = partitionOf("remote-terminal");
        long taskId = store.create("mock", partition, NODE_A, NODE_B);
        for (MigrationTaskState state : List.of(MigrationTaskState.PREPARING,
                MigrationTaskState.SNAPSHOT, MigrationTaskState.COPYING,
                MigrationTaskState.SYNCING, MigrationTaskState.CUTOVER,
                MigrationTaskState.VERIFYING, MigrationTaskState.COMPLETED)) {
            store.transition(taskId, state);
        }

        byte[] data = MockPartitionAdapter.encodeEntry("dup-key-" + partition, "v1");
        try (RemotePartitionAdapter bound =
                     new RemotePartitionAdapter("localhost", host.getPort(), taskId)) {
            assertThat(bound.importChunks("mock", partition, List.of(data))).isFalse();
        }
        assertThat(capability.partitionCount(partition)).isZero();
    }

    @Test
    void importChunksSurfacesFailedPreconditionWithStatusCodeInCauseChain() throws Exception {
        // 接收端能力未启动：transferStream 以 FAILED_PRECONDITION 终止流。
        // 客户端必须把 StatusRuntimeException 保留在 cause 链中抛出（不能吞成 false）——
        // 引擎 isPreconditionFailure 沿 cause 链识别后按「启动窗口延后」降级，不打 WARN+堆栈
        PartitionDataAdapter notStarted = new PartitionDataAdapter() {
            @Override
            public long itemCount(String capability, int partition) {
                return 0;
            }

            @Override
            public Iterator<byte[]> exportChunks(String capability, int partition,
                                                 long fromCheckpoint) {
                return java.util.Collections.emptyIterator();
            }

            @Override
            public boolean importChunks(String capability, int partition, List<byte[]> chunks) {
                throw new com.ww.runtime.common.RuntimeFrameworkException(
                        "cache capability not started");
            }

            @Override
            public long verifyCount(String capability, int partition) {
                return 0;
            }
        };
        InMemoryMigrationTaskStore throwStore = new InMemoryMigrationTaskStore();
        long taskId = throwStore.create("mock", partitionOf("remote-precond"), NODE_A, NODE_B);
        GrpcServerHost throwHost = new GrpcServerHost();
        throwHost.addService(new MigrationServer(notStarted, throwStore, new TestClock()));
        throwHost.start(0);
        byte[] data = MockPartitionAdapter.encodeEntry("precond-key", "v1");
        try (RemotePartitionAdapter bound =
                     new RemotePartitionAdapter("localhost", throwHost.getPort(), taskId)) {
            assertThatThrownBy(() -> bound.importChunks("mock", 0, List.of(data)))
                    .isInstanceOf(com.ww.runtime.common.MigrationException.class)
                    .satisfies(e -> {
                        boolean found = false;
                        for (Throwable t = e; t != null; t = t.getCause()) {
                            if (t instanceof io.grpc.StatusRuntimeException sre
                                    && sre.getStatus().getCode()
                                    == io.grpc.Status.Code.FAILED_PRECONDITION) {
                                found = true;
                            }
                        }
                        assertThat(found)
                                .as("cause chain must contain FAILED_PRECONDITION StatusRuntimeException")
                                .isTrue();
                    });
        } finally {
            throwHost.stop();
            throwHost.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void importChunksBacksOffExponentiallyBetweenResendRounds() throws Exception {
        // 接收端前两轮 ok=false、第三轮接受：重发轮次间必须按 25ms、50ms 指数退避，
        // 不能在同一瞬态窗口内毫秒级打满 3 次重试预算（启动窗口场景的原始问题）
        FlakyImportAdapter flaky = new FlakyImportAdapter(
                new MockPartitionAdapter(capability), 2);
        InMemoryMigrationTaskStore flakyStore = new InMemoryMigrationTaskStore();
        int partition = partitionOf("remote-backoff");
        long taskId = flakyStore.create("mock", partition, NODE_A, NODE_B);
        GrpcServerHost flakyHost = new GrpcServerHost();
        flakyHost.addService(new MigrationServer(flaky, flakyStore, new TestClock()));
        flakyHost.start(0);
        byte[] data = MockPartitionAdapter.encodeEntry(keyOfPartition(partition), "v1");
        List<Long> backoffSleeps = new ArrayList<>();
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", flakyHost.getPort())
                .usePlaintext().build();
        try (RemotePartitionAdapter bound = new RemotePartitionAdapter(
                channel, taskId, backoffSleeps::add)) {
            boolean ok = bound.importChunks("mock", partition, List.of(data));

            assertThat(ok).isTrue();
            assertThat(flaky.importCalls.get()).isEqualTo(3);
            assertThat(backoffSleeps).containsExactly(25L, 50L); // 指数退避：base、2×base
        } finally {
            channel.shutdownNow();
            flakyHost.stop();
            flakyHost.awaitTermination(5, TimeUnit.SECONDS);
        }
        assertThat(capability.partitionCount(partition)).isEqualTo(1); // 第三轮落盘
    }

    /** 找到一个真正落入指定分区的 key（Mock 导入会拒绝跨分区 key）。 */
    private static String keyOfPartition(int partition) {
        for (int i = 0; ; i++) {
            String key = "key-of-" + partition + "-" + i;
            if (Partitioner.partition(key, PARTITION_COUNT) == partition) {
                return key;
            }
        }
    }

    /** 前 failRounds 次 importChunks 返回 false（接收端回 ok=false 要求重发），之后委托真实适配器。 */
    private static final class FlakyImportAdapter implements PartitionDataAdapter {
        private final PartitionDataAdapter delegate;
        private final int failRounds;
        private final AtomicInteger importCalls = new AtomicInteger();

        FlakyImportAdapter(PartitionDataAdapter delegate, int failRounds) {
            this.delegate = delegate;
            this.failRounds = failRounds;
        }

        @Override
        public long itemCount(String capability, int partition) {
            return delegate.itemCount(capability, partition);
        }

        @Override
        public Iterator<byte[]> exportChunks(String capability, int partition, long fromCheckpoint) {
            return delegate.exportChunks(capability, partition, fromCheckpoint);
        }

        @Override
        public boolean importChunks(String capability, int partition, List<byte[]> chunks) {
            if (importCalls.incrementAndGet() <= failRounds) {
                return false;
            }
            return delegate.importChunks(capability, partition, chunks);
        }

        @Override
        public long verifyCount(String capability, int partition) {
            return delegate.verifyCount(capability, partition);
        }
    }

    @Test
    void clearPartitionReturnsFalseWhenReceiverReportsFailure() throws Exception {
        // 远端 clearPartition 失败经 ack ok=false 透传为 false；
        // 拒绝原因（ack message）由客户端 WARN 打出，引擎按启动清扫兜底
        PartitionDataAdapter failing = new PartitionDataAdapter() {
            @Override
            public long itemCount(String capability, int partition) {
                return 0;
            }

            @Override
            public Iterator<byte[]> exportChunks(String capability, int partition,
                                                 long fromCheckpoint) {
                return java.util.Collections.<byte[]>emptyIterator();
            }

            @Override
            public boolean importChunks(String capability, int partition, List<byte[]> chunks) {
                return true;
            }

            @Override
            public long verifyCount(String capability, int partition) {
                return 0;
            }

            @Override
            public boolean clearPartition(String capability, int partition, NodeId source) {
                return false;
            }
        };
        GrpcServerHost failingHost = new GrpcServerHost();
        failingHost.addService(new MigrationServer(failing, store, new TestClock()));
        failingHost.start(0);
        try {
            try (RemotePartitionAdapter remote =
                         new RemotePartitionAdapter("localhost", failingHost.getPort())) {
                assertThat(remote.clearPartition("mock", partitionOf("remote-clear-fail")))
                        .isFalse();
            }
        } finally {
            failingHost.stop();
            failingHost.awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
