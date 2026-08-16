package com.ww.rpc.core;

/**
 * 目标节点不可达/不可用异常。
 */
public class RpcUnavailableException extends RpcException {

    public RpcUnavailableException(String message) {
        super(message);
    }

    public RpcUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
