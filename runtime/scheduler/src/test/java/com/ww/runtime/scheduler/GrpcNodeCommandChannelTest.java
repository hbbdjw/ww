package com.ww.runtime.scheduler;

import com.ww.runtime.capability.CapabilityManager;
import com.ww.runtime.capability.InMemoryCapabilityStateStore;
import com.ww.runtime.capability.MockCapability;
import com.ww.runtime.cluster.InMemoryNodeRegistry;
import com.ww.runtime.common.CapabilityState;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.NodeInfo;
import com.ww.runtime.common.NodeState;
import com.ww.runtime.common.ResourceSnapshot;
import com.ww.runtime.common.SystemClock;
import com.ww.runtime.grpc.GrpcServerHost;
import com.ww.runtime.proto.Ack;
import com.ww.runtime.proto.ClusterControlGrpc;
import com.ww.runtime.proto.FreezeReq;
import com.ww.runtime.proto.StartCapabilityReq;
import com.ww.runtime.proto.StopCapabilityReq;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

class GrpcNodeCommandChannelTest {

    private static final NodeId SELF = NodeId.of("self");
    private static final NodeId REMOTE = NodeId.of("remote-1");

    private GrpcServerHost serverHost;
    private FakeClusterControl fakeService;
    private InMemoryNodeRegistry registry;
    private CapabilityManager manager;
    private GrpcNodeCommandChannel channel;

    @BeforeEach
    void setUp() throws Exception {
        fakeService = new FakeClusterControl();
        serverHost = new GrpcServerHost().addService(fakeService);
        serverHost.start(0);

        registry = new InMemoryNodeRegistry();
        registry.register(NodeInfo.of(REMOTE, "localhost", serverHost.getPort(),
                NodeState.JOINING, 0L, 1.0d,
                ResourceSnapshot.of(1.0d, 100, 10, 100, 10, SystemClock.instance().nowMillis())));

        var store = new InMemoryCapabilityStateStore(SystemClock.instance());
        manager = new CapabilityManager(SELF, store, SystemClock.instance());
        manager.register(new MockCapability(SELF, (cap, partition) -> Optional.empty(), key -> 0));
        LocalNodeCommandChannel local = new LocalNodeCommandChannel(SELF, manager);

        channel = new GrpcNodeCommandChannel(SELF, local, registry);
    }

    @AfterEach
    void tearDown() {
        channel.close();
        serverHost.stop();
    }

    @Test
    void remoteCommandsAreDispatchedOverGrpc() {
        assertThat(channel.startCapability(REMOTE, CapabilityType.MOCK)).isTrue();
        assertThat(fakeService.started).containsExactly(REMOTE.value() + ":MOCK");

        assertThat(channel.freeze(REMOTE, CapabilityType.MOCK, true)).isTrue();
        assertThat(fakeService.frozen).containsEntry(REMOTE.value() + ":MOCK", true);

        assertThat(channel.stopCapability(REMOTE, CapabilityType.MOCK)).isTrue();
        assertThat(fakeService.stopped).containsExactly(REMOTE.value() + ":MOCK");
    }

    @Test
    void selfNodeFallsBackToLocalChannel() {
        assertThat(channel.startCapability(SELF, CapabilityType.MOCK)).isTrue();

        assertThat(manager.state(CapabilityType.MOCK)).isEqualTo(CapabilityState.ACTIVE);
        assertThat(fakeService.started).isEmpty(); // 未绕网络
    }

    @Test
    void unknownNodeReturnsFalse() {
        assertThat(channel.startCapability(NodeId.of("ghost"), CapabilityType.MOCK)).isFalse();
        assertThat(channel.stopCapability(NodeId.of("ghost"), CapabilityType.MOCK)).isFalse();
        assertThat(channel.freeze(NodeId.of("ghost"), CapabilityType.MOCK, true)).isFalse();
    }

    /** 记录命令并恒定返回 ok 的假 ClusterControl 服务。 */
    private static final class FakeClusterControl extends ClusterControlGrpc.ClusterControlImplBase {

        private final java.util.List<String> started = new java.util.concurrent.CopyOnWriteArrayList<>();
        private final java.util.List<String> stopped = new java.util.concurrent.CopyOnWriteArrayList<>();
        private final Map<String, Boolean> frozen = new ConcurrentHashMap<>();

        @Override
        public void startCapability(StartCapabilityReq request, StreamObserver<Ack> responseObserver) {
            started.add(request.getNodeId() + ":" + request.getCapability());
            respond(responseObserver, "started");
        }

        @Override
        public void stopCapability(StopCapabilityReq request, StreamObserver<Ack> responseObserver) {
            stopped.add(request.getNodeId() + ":" + request.getCapability());
            respond(responseObserver, "stopped");
        }

        @Override
        public void freeze(FreezeReq request, StreamObserver<Ack> responseObserver) {
            frozen.put(request.getNodeId() + ":" + request.getCapability(), request.getFrozen());
            respond(responseObserver, "frozen=" + request.getFrozen());
        }

        private static void respond(StreamObserver<Ack> responseObserver, String message) {
            responseObserver.onNext(Ack.newBuilder().setOk(true).setMessage(message).build());
            responseObserver.onCompleted();
        }
    }
}
