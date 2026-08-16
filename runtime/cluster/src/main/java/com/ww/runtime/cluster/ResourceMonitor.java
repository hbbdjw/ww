package com.ww.runtime.cluster;

import com.ww.runtime.common.ResourceSnapshot;

/**
 * 节点资源监控：为心跳与调度权重提供本机资源视图。
 */
public interface ResourceMonitor {

    /** 采集一次资源快照（CPU / 内存 / 磁盘 + 采样时刻）。 */
    ResourceSnapshot snapshot();
}
