package com.ww.runtime.common;

/**
 * 节点静态/动态信息聚合。
 *
 * @param httpPort 节点 REST 数据面端口（负载均衡反代寻址用）；0 表示未知/未上报，
 *                 此时不参与 LB 分发；gRPC 寻址仍使用 {@link #grpcPort}
 * @param zone     故障域标识（机架/可用区/主机组）；空串 = 未配置（放置不做反亲和）
 */
public record NodeInfo(
        NodeId id,
        String host,
        int grpcPort,
        int httpPort,
        NodeState state,
        long stateEpoch,
        double weight,
        ResourceSnapshot resources,
        String zone) {

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
        if (httpPort < 0 || httpPort > 65535) {
            throw new IllegalArgumentException("httpPort must be in [0, 65535], got " + httpPort);
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
        if (zone == null) {
            zone = "";
        }
    }

    /** 兼容构造：未配置故障域。 */
    public NodeInfo(NodeId id, String host, int grpcPort, int httpPort, NodeState state,
                    long stateEpoch, double weight, ResourceSnapshot resources) {
        this(id, host, grpcPort, httpPort, state, stateEpoch, weight, resources, "");
    }

    /** 兼容工厂：httpPort 未知（0），不参与 LB 分发。 */
    public static NodeInfo of(NodeId id,
                              String host,
                              int grpcPort,
                              NodeState state,
                              long stateEpoch,
                              double weight,
                              ResourceSnapshot resources) {
        return new NodeInfo(id, host, grpcPort, 0, state, stateEpoch, weight, resources, "");
    }

    public static NodeInfo of(NodeId id,
                              String host,
                              int grpcPort,
                              int httpPort,
                              NodeState state,
                              long stateEpoch,
                              double weight,
                              ResourceSnapshot resources) {
        return new NodeInfo(id, host, grpcPort, httpPort, state, stateEpoch, weight, resources, "");
    }

    /** 带故障域的工厂（httpPort 未知）。 */
    public static NodeInfo of(NodeId id, String host, int grpcPort, NodeState state,
                              long stateEpoch, double weight, ResourceSnapshot resources,
                              String zone) {
        return new NodeInfo(id, host, grpcPort, 0, state, stateEpoch, weight, resources, zone);
    }

    /** 带故障域的工厂。 */
    public static NodeInfo of(NodeId id, String host, int grpcPort, int httpPort,
                              NodeState state, long stateEpoch, double weight,
                              ResourceSnapshot resources, String zone) {
        return new NodeInfo(id, host, grpcPort, httpPort, state, stateEpoch, weight, resources, zone);
    }

    /** 同节点信息替换状态（保留故障域等静态字段）。 */
    public NodeInfo withState(NodeState newState, long newEpoch,
                              ResourceSnapshot newResources) {
        return new NodeInfo(id, host, grpcPort, httpPort, newState, newEpoch, weight,
                newResources, zone);
    }
}
