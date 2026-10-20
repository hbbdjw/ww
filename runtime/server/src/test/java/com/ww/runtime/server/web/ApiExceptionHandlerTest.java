package com.ww.runtime.server.web;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.context.request.async.AsyncRequestNotUsableException;

/**
 * {@link ApiExceptionHandler} 单测：重点锁定 SSE/流式响应客户端断开
 * （{@link AsyncRequestNotUsableException}）必须静默消化——不抛异常、不刷 ERROR 日志，
 * 同时通用未预期异常仍按 500 + ERROR 堆栈处理，防止降噪逻辑误伤真实故障。
 */
class ApiExceptionHandlerTest {

    private ApiExceptionHandler handler;
    private ListAppender<ILoggingEvent> appender;
    private Logger logger;

    @BeforeEach
    void setUp() {
        handler = new ApiExceptionHandler();
        logger = (Logger) LoggerFactory.getLogger(ApiExceptionHandler.class);
        appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
    }

    @AfterEach
    void tearDown() {
        logger.detachAppender(appender);
    }

    @Test
    void asyncDisconnectIsSwallowedQuietly() {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getMethod()).thenReturn("GET");
        when(request.getRequestURI()).thenReturn("/rt/status/stream");
        AsyncRequestNotUsableException ex = new AsyncRequestNotUsableException(
                "Servlet container error notification for disconnected client");

        // 连接已不可写：处理过程不应再抛出（否则会被容器当作二次错误），且无返回体
        assertThatCode(() -> handler.asyncRequestNotUsable(ex, request))
                .doesNotThrowAnyException();

        List<ILoggingEvent> events = appender.list;
        assertThat(events).noneMatch(e -> e.getLevel() == Level.ERROR);
        assertThat(events).noneMatch(e -> e.getLevel() == Level.WARN);
    }

    @Test
    void genericUnexpectedExceptionStillReturns500AndLogsError() {
        var response = handler.unexpected(new IllegalStateException("boom"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(appender.list).anyMatch(e -> e.getLevel() == Level.ERROR);
    }
}
