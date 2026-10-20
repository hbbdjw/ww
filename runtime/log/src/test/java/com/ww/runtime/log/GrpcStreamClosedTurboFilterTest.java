package com.ww.runtime.log;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class GrpcStreamClosedTurboFilterTest {

    private LoggerContext context;
    private Logger logger;
    private ListAppender<ILoggingEvent> list;

    @BeforeEach
    void setUp() {
        context = new LoggerContext();
        GrpcStreamClosedTurboFilter filter = new GrpcStreamClosedTurboFilter();
        filter.setContext(context);
        filter.start();
        context.addTurboFilter(filter);

        logger = context.getLogger("io.grpc.netty.NettyServerHandler");
        logger.setLevel(Level.WARN);
        logger.setAdditive(false);
        list = new ListAppender<>();
        list.setContext(context);
        list.start();
        logger.addAppender(list);
    }

    @AfterEach
    void tearDown() {
        list.stop();
        context.stop();
    }

    @Test
    void streamClosedWarnIsReplacedWithDescriptionWithoutStack() {
        logger.warn("Stream Error", new RuntimeException(
                "Stream closed before write could take place"));

        assertThat(list.list).hasSize(1);
        ILoggingEvent event = list.list.get(0);
        assertThat(event.getLevel()).isEqualTo(Level.WARN);
        assertThat(event.getFormattedMessage()).isEqualTo(GrpcStreamClosedTurboFilter.DESCRIPTION);
        assertThat(event.getThrowableProxy()).isNull();
    }

    @Test
    void wrappedCauseIsAlsoReplaced() {
        RuntimeException wrapped = new RuntimeException("grpc failed",
                new RuntimeException("Stream closed before write could take place"));
        logger.warn("Stream Error", wrapped);

        assertThat(list.list).hasSize(1);
        assertThat(list.list.get(0).getThrowableProxy()).isNull();
        assertThat(list.list.get(0).getFormattedMessage())
                .isEqualTo(GrpcStreamClosedTurboFilter.DESCRIPTION);
    }

    @Test
    void unrelatedThrowableKeepsMessageAndStack() {
        RuntimeException boom = new RuntimeException("boom");
        logger.warn("other error", boom);

        assertThat(list.list).hasSize(1);
        ILoggingEvent event = list.list.get(0);
        assertThat(event.getFormattedMessage()).isEqualTo("other error");
        assertThat(event.getThrowableProxy()).isNotNull();
        assertThat(event.getThrowableProxy().getMessage()).isEqualTo("boom");
    }
}
