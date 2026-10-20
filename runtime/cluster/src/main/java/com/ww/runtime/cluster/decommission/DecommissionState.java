package com.ww.runtime.cluster.decommission;

/**
 * 退役流程状态。
 *
 * <pre>
 * DRAINING ──gate──&gt; REMOVING ──&gt; COMPLETED
 *    │                   │
 *    └──&gt; FAILED &lt;───────┘
 * </pre>
 */
public enum DecommissionState {

    DRAINING,
    REMOVING,
    COMPLETED,
    FAILED;

    public boolean terminal() {
        return this == COMPLETED || this == FAILED;
    }
}
