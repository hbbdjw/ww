package com.ww.runtime.grpc;

import io.grpc.ServerServiceDefinition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GrpcServerHostTest {

    private GrpcServerHost host;

    @AfterEach
    void tearDown() {
        if (host != null) {
            host.stop();
            try {
                host.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static io.grpc.BindableService dummyService(String name) {
        return () -> ServerServiceDefinition.builder(name).build();
    }

    @Test
    void startWithPortZeroBindsRandomPort() throws Exception {
        host = new GrpcServerHost();
        host.addService(dummyService("test.Dummy"));
        assertThat(host.isRunning()).isFalse();

        host.start(0);
        assertThat(host.isRunning()).isTrue();
        assertThat(host.getPort()).isPositive();

        host.stop();
        host.awaitTermination(5, TimeUnit.SECONDS);
        assertThat(host.isRunning()).isFalse();
        assertThatThrownBy(host::getPort).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void doubleStartThrowsIllegalState() throws Exception {
        host = new GrpcServerHost();
        host.start(0);
        int firstPort = host.getPort();

        assertThatThrownBy(() -> host.start(0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already started");
        // 原服务器未受影响
        assertThat(host.getPort()).isEqualTo(firstPort);
    }

    @Test
    void addServiceAfterStartThrowsIllegalState() throws Exception {
        host = new GrpcServerHost();
        host.start(0);

        assertThatThrownBy(() -> host.addService(dummyService("test.Late")))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void addServiceRejectsNull() {
        host = new GrpcServerHost();
        assertThatThrownBy(() -> host.addService(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void restartAfterStopSucceeds() throws Exception {
        host = new GrpcServerHost();
        host.addService(dummyService("test.Restart"));
        host.start(0);
        int firstPort = host.getPort();

        host.stop();
        host.awaitTermination(5, TimeUnit.SECONDS);

        host.start(0);
        // 随机端口重启后应能再次读取端口（两次端口可能不同，不做相等断言）
        assertThat(host.getPort()).isPositive();
        assertThat(firstPort).isPositive();
    }
}
