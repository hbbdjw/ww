package com.ww.runtime.log;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.turbo.TurboFilter;
import ch.qos.logback.core.spi.FilterReply;
import org.slf4j.Marker;

/**
 * 将 Netty/gRPC 的「Stream closed before write could take place」从带堆栈的 WARN
 * 改写为一条无堆栈描述（心跳短流半关闭竞态，可忽略）。
 */
public class GrpcStreamClosedTurboFilter extends TurboFilter {

    static final String MARKER = "Stream closed before write could take place";
    static final String DESCRIPTION =
            "gRPC HTTP/2 流在写入前已关闭（对端半关闭或取消，常见于心跳短流竞态，可忽略）";

    @Override
    public FilterReply decide(Marker marker, Logger logger, Level level,
                              String format, Object[] params, Throwable t) {
        if (!isStarted() || !isStreamClosedBeforeWrite(t)) {
            return FilterReply.NEUTRAL;
        }
        logger.warn(DESCRIPTION);
        return FilterReply.DENY;
    }

    static boolean isStreamClosedBeforeWrite(Throwable t) {
        for (Throwable cur = t; cur != null; cur = cur.getCause()) {
            String msg = cur.getMessage();
            if (msg != null && msg.contains(MARKER)) {
                return true;
            }
        }
        return false;
    }
}
