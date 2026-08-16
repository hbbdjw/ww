package com.ww.storage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ww.common.ClusterNode;
import com.ww.common.RendezvousHash;

/**
 * 节点路由器：基于 HRW（最高随机权重哈希）为 (bucket,key) 选定主节点与副本节点。
 *
 * <p>复用 cache 模块经过验证的 {@link RendezvousHash} 算法，仅将 StorageNode 适配为
 * ClusterNode 视图（port=httpPort，确保 nodeKey 与权重参与哈希计算）。
 *
 * <p>所有节点等层级关系：任一节点收到请求后，若自身非主则按 HRW 转发到主节点，
 * 由主节点本地写 + 同步复制到副本节点，保证 read-after-write 一致性。
 *
 * <p><b>动态扩缩容支持</b>：内部状态改为 volatile 非 final，提供 {@link #rebuild} 方法
 * 在拓扑变更时重建 HRW。重建期间读旧快照可见，重建完成后原子切换到新快照
 * （参考 cache 模块 {@code DistributedCache.applyTopologyFromMembership} 语义）。
 */
public class NodeRouter {

    private volatile List<StorageNode> nodes;
    private volatile Map<String, StorageNode> byKey;
    private volatile RendezvousHash hrw;
    private volatile int replicas;
    private final StorageNode self;
    private volatile boolean distributed;

    public NodeRouter(List<StorageNode> nodes, int replicas) {
        if (nodes == null || nodes.isEmpty()) {
            throw new StorageException("NodeRouter: 节点列表为空");
        }
        StorageNode selfNode = null;
        for (StorageNode sn : nodes) {
            if (sn.isSelf()) {
                selfNode = sn;
                break;
            }
        }
        if (selfNode == null) {
            throw new StorageException("NodeRouter: 未标记 self 节点");
        }
        this.self = selfNode;
        rebuild(nodes, replicas);
    }

    /**
     * 用新节点列表重建路由。供 {@code StorageTopologyManager} 在扩缩容时调用。
     * <p>线程安全：构造新快照后原子替换引用，读路径无锁。
     *
     * @param newNodes 新节点列表（必须含 self）
     * @param replicas 副本数
     */
    public synchronized void rebuild(List<StorageNode> newNodes, int replicas) {
        if (newNodes == null || newNodes.isEmpty()) {
            throw new StorageException("NodeRouter.rebuild: 节点列表为空");
        }
        boolean selfFound = false;
        for (StorageNode sn : newNodes) {
            if (sn.isSelf()) {
                selfFound = true;
                break;
            }
        }
        if (!selfFound) {
            throw new StorageException("NodeRouter.rebuild: 新节点列表未含 self 节点");
        }
        this.nodes = Collections.unmodifiableList(new ArrayList<>(newNodes));
        Map<String, StorageNode> k = new HashMap<>();
        List<ClusterNode> adapters = new ArrayList<>(newNodes.size());
        for (StorageNode sn : newNodes) {
            k.put(sn.nodeKey(), sn);
            ClusterNode cn = new ClusterNode(sn.getHost(), sn.getHttpPort(), sn.getWeight());
            cn.setSelf(sn.isSelf());
            adapters.add(cn);
        }
        this.byKey = Collections.unmodifiableMap(k);
        this.hrw = new RendezvousHash(adapters);
        int r = Math.max(1, replicas);
        if (r > newNodes.size()) {
            r = newNodes.size();
        }
        this.replicas = r;
        // 单节点视为非分布式：直接走本地，避免无谓的 HTTP 自调用
        this.distributed = newNodes.size() > 1;
    }

    public boolean isDistributed() { return distributed; }

    public StorageNode getSelf() { return self; }

    public List<StorageNode> getNodes() { return nodes; }

    /** 按 nodeKey 查节点（供复制补偿回放/对账定位副本） */
    public StorageNode getNode(String nodeKey) {
        return byKey.get(nodeKey);
    }

    public int getReplicas() { return replicas; }

    /** 选主+副本列表（按 HRW 降序），[0]=primary */
    public List<StorageNode> selectOwners(String routingKey) {
        List<ClusterNode> selected = hrw.selectNodes(routingKey, replicas);
        List<StorageNode> result = new ArrayList<>(selected.size());
        for (ClusterNode cn : selected) {
            StorageNode sn = byKey.get(cn.nodeKey());
            if (sn != null) result.add(sn);
        }
        return result;
    }

    public StorageNode selectPrimary(String routingKey) {
        List<StorageNode> owners = selectOwners(routingKey);
        return owners.isEmpty() ? null : owners.getFirst();
    }

    /** 是否本节点为主 */
    public boolean isPrimary(String routingKey) {
        StorageNode p = selectPrimary(routingKey);
        return p != null && p.isSelf();
    }

    /** 副本节点（不含主） */
    public List<StorageNode> selectReplicas(String routingKey) {
        List<StorageNode> owners = selectOwners(routingKey);
        if (owners.size() <= 1) return Collections.emptyList();
        return new ArrayList<>(owners.subList(1, owners.size()));
    }
}
