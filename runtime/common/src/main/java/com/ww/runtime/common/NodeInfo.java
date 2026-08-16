package com.ww.runtime.common;

/**
 * 节点静态/动态信息聚合。
 */
public record NodeInfo(
        NodeId id,
        String host,
        int grpcPort,
        NodeState state,
        long stateEpoch,
        double weight,
        ResourceSnapshot resources) {

    public NodeInfo {
        if (id == null) {
            throw new IllegalArgumentException("NodeInfo.id must not be null");
        }
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("NodeInfo.host must not be null or blank");
        }
        if (grpcPort < 0 || grpcPort > 65535) {
            throw new IllegalArgumentException("grpcPort must be in [0, 65535], got " + grpcPort);
        }
        if (state == null) {
            throw new IllegalArgumentException("NodeInfo.state must not be null");
        }
        if (!Double.isFinite(weight) || weight <= 0) {
            throw new IllegalArgumentException("weight must be positive finite, got " + weight);
        }
        if (resources == null) {
            throw new IllegalArgumentException("NodeInfo.resources must not be null");
        }
    }

    public static NodeInfo of(NodeId id,
                              String host,
                              int grpcPort,
                              NodeState state,
                              long stateEpoch,
                              double weight,
                              ResourceSnapshot resources) {
        return new NodeInfo(id, host, grpcPort, state, stateEpoch, weight, resources);
    }
}
