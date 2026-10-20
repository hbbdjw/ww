package com.ww.runtime.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ww.runtime.common.RuntimeFrameworkException;
import com.ww.runtime.common.StaleEpochException;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * 数据面网关共享错误映射（Cache/Storage 网关同约定）：
 * gRPC 状态 → 框架异常（REST 层按类型还原 HTTP 语义）。
 *
 * <ul>
 *   <li>{@code ABORTED + stale-epoch} → {@link StaleEpochException}（HTTP 409）；</li>
 *   <li>{@code ABORTED + not-owner} → {@link StaleEpochException}（路由错位，
 *       调用端重取属主重试可收敛，HTTP 语义同 409）；</li>
 *   <li>{@code UNAVAILABLE + frozen} → RuntimeFrameworkException("frozen")（HTTP 423）；</li>
 *   <li>其余 → RuntimeFrameworkException（HTTP 503）。</li>
 * </ul>
 */
public final class GatewayErrors {

    private static final Logger LOG = LoggerFactory.getLogger(GatewayErrors.class);

    private GatewayErrors() {
    }

    /** gRPC 状态 → 框架异常（REST 层按类型还原 HTTP 语义）。 */
    public static RuntimeFrameworkException translate(StatusRuntimeException e, String op, String target) {
        Status status = e.getStatus();
        String desc = status.getDescription() == null ? "" : status.getDescription();
        if (status.getCode() == Status.Code.ABORTED && desc.startsWith("stale-epoch")) {
            return new StaleEpochException(desc);
        }
        if (status.getCode() == Status.Code.ABORTED && desc.startsWith("not-owner")) {
            // 转发读路由错位（cutover 期间属主已变更）：还原为可重路由的 409 语义
            return new StaleEpochException(desc);
        }
        if (status.getCode() == Status.Code.UNAVAILABLE && desc.startsWith("frozen")) {
            return new RuntimeFrameworkException("frozen");
        }
        LOG.warn("gateway {} {} failed: {} {}", op, target, status.getCode(), desc);
        return new RuntimeFrameworkException("gateway " + op + " failed: " + status.getCode()
                + (desc.isEmpty() ? "" : " " + desc));
    }

    /** 本地能力调用抛出的可重路由错误（stale-epoch / not-owner 语义）。 */
    public static boolean isReroutable(RuntimeFrameworkException e) {
        return e instanceof StaleEpochException;
    }
}
