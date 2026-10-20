package com.ww.runtime.lb;

import java.util.Locale;
import java.util.Objects;

/**
 * LB 视角的节点只读视图（由宿主侧适配集群注册表得到，runtime-lb 不依赖集群模块）。
 *
 * @param nodeId    节点标识
 * @param host      对外可访问主机名/IP（注册表通告地址）
 * @param httpPort  节点 REST 数据面端口；{@code <=0} 表示未知，不参与均衡
 * @param weight    节点注册权重（恒正）
 * @param active    是否处于可服务状态（注册表 ACTIVE；JOINING/SUSPECTED/DEAD/LEAVING 为 false）
 */
public record LbNodeView(String nodeId, String host, int httpPort, double weight, boolean active) {

    public LbNodeView {
        Objects.requireNonNull(nodeId, "nodeId");
        if (nodeId.isBlank()) {
            throw new IllegalArgumentException("nodeId must not be blank");
        }
        Objects.requireNonNull(host, "host");
        if (host.isBlank()) {
            throw new IllegalArgumentException("host must not be blank");
        }
        if (httpPort < 0 || httpPort > 65535) {
            throw new IllegalArgumentException("httpPort must be in [0, 65535], got " + httpPort);
        }
        if (!Double.isFinite(weight) || weight <= 0) {
            throw new IllegalArgumentException("weight must be positive finite, got " + weight);
        }
    }

    public static LbNodeView active(String nodeId, String host, int httpPort, double weight) {
        return new LbNodeView(nodeId, host, httpPort, weight, true);
    }

    /** 反代目标基地址，如 {@code http://rt-node2:8092}。host 为 https 前缀时按原样保留协议。 */
    public String baseUrl() {
        String h = host;
        if (h.startsWith("http://") || h.startsWith("https://")) {
            return stripTrailingSlash(h) + ":" + httpPort;
        }
        return "http://" + h + ":" + httpPort;
    }

    private static String stripTrailingSlash(String s) {
        return s.endsWith("/") ? s.substring(0, s.length() - 1) : s;
    }

    String lowerId() {
        return nodeId.toLowerCase(Locale.ROOT);
    }
}
