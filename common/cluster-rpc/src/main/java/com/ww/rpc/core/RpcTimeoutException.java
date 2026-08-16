package com.ww.rpc.core;

/**
 * RPC 请求超时异常。
 */
public class RpcTimeoutException extends RpcException {

    public RpcTimeoutException(String message) {
        super(message);
    }

    public RpcTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
