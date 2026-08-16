package com.ww.rpc.core;

/**
 * 非主节点异常：当请求发到非 Leader/非 Primary 节点时抛出，
 * 包含 leaderHint 提示客户端应重定向到的节点地址。
 */
public class NotLeaderException extends RpcException {

    private final String leaderHint;

    public NotLeaderException(String leaderHint) {
        super("not leader, redirect to: " + (leaderHint == null ? "<unknown>" : leaderHint));
        this.leaderHint = leaderHint;
    }

    public NotLeaderException(String leaderHint, String message) {
        super(message);
        this.leaderHint = leaderHint;
    }

    public String getLeaderHint() {
        return leaderHint;
    }
}
