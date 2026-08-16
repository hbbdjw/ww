package com.ww.runtime.membership;

import com.ww.runtime.common.NodeInfo;

import java.util.List;

/**
 * 对端目录：返回除自身外当前应当心跳的存活节点。
 * <p>宿主侧通常适配 {@code NodeRegistry.listAlive()}。</p>
 */
@FunctionalInterface
public interface PeerDirectory {

    /** 除自身外的存活节点列表。 */
    List<NodeInfo> peers();
}
