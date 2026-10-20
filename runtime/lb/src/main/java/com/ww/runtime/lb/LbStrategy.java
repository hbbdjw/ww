package com.ww.runtime.lb;

import java.util.Locale;

/**
 * 负载均衡分发策略。
 */
public enum LbStrategy {

    /** 轮询：候选节点按 nodeId 稳定排序后依次轮转。 */
    ROUND_ROBIN,

    /** 平滑权重：nginx 平滑加权轮询，权重来自页面覆盖或节点注册权重。 */
    WEIGHTED,

    /** 随机：候选节点等概率随机（不考虑权重）。 */
    RANDOM;

    /** 宽容解析（大小写不敏感），非法值抛 IllegalArgumentException。 */
    public static LbStrategy parse(String raw) {
        if (raw == null || raw.isBlank()) {
            throw new IllegalArgumentException("strategy must not be blank");
        }
        return valueOf(raw.trim().toUpperCase(Locale.ROOT));
    }
}
