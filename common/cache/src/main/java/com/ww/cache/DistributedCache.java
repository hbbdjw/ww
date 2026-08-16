package com.ww.cache;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import com.ww.common.ClusterNode;
import com.ww.common.RendezvousHash;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 分布式缓存：HRW 路由 + 主从复制（异步但带 backlog 重放补偿）。
 *
 * <p>写流程：HRW 选主 → 主节点本地存储 → 异步复制到备节点；备节点不健康时写命令进入 backlog 队列，
 * 节点恢复后由健康检查线程回放 backlog。
 * <p>读流程：HRW 选主 → 主节点读取（按读策略 fallback 到备节点，PRIMARY_ONLY 不 fallback）。
 * <p>读策略：通过 {@link #withReadPolicy(ReadPolicy)} 切换线程局部策略，
 * 默认 {@link ReadPolicy#FAVOR_PRIMARY}（主优先、失败 fallback，会收到陈旧告警）。
 */
public class DistributedCache implements Cache<String, Object> {

    private static final Logger log = LoggerFactory.getLogger(DistributedCache.class);

    /** 单节点 backlog 最大长度，防止异常节点堆积无限内存 {@code 缺陷1修复} */
    private static final int BACKLOG_MAX_PER_NODE = 10_000;

    private final SimpleCache<String, Object> localCache;
    private volatile RendezvousHash hrw;
    private volatile int replicas;
    private volatile ClusterNode self;
    private final String authToken;
    /** 客户端工厂：默认创建 TCP CacheClient；可注入 gRPC 等其他实现 */
    private final Function<ClusterNode, ? extends CacheRpcClient> clientFactory;
    private final Map<String, CacheRpcClient> primaryClientPool = new ConcurrentHashMap<>();
    private final Map<String, CacheRpcClient> replicaClientPool = new ConcurrentHashMap<>();
    private final Map<String, Long> lastReplicaWarnMs = new ConcurrentHashMap<>();
    private final Set<String> unhealthyNodes = ConcurrentHashMap.newKeySet();
    /** 每个备份节点的待重放复制命令队列 {@code 缺陷1修复} */
    private final Map<String, ConcurrentLinkedQueue<PendingReplica>> replicationBacklog = new ConcurrentHashMap<>();
    /** 每个 key 的 pop 串行化锁 {@code 缺陷4修复}，使用 WeakHashMap 自动清理不再使用的锁 */
    private final Map<String, ReentrantLock> keyLocks = Collections.synchronizedMap(new WeakHashMap<>());
    private final ThreadLocal<ReadPolicy> readPolicyTL = ThreadLocal.withInitial(() -> ReadPolicy.FAVOR_PRIMARY);
    private ScheduledExecutorService healthChecker;
    /** 集群动态成员管理（可选；未启用时分布式缓存仍按静态 HRW 路由） */
    private volatile ClusterMembership membership;
    /** failover 管理器（可选；启动后周期性探测对端） */
    private volatile FailoverManager failoverManager;
    /** 数据重平衡管理器（可选；启动后监听 topologyVersion 变化触发迁移） */
    private volatile RebalanceManager rebalanceManager;

    public DistributedCache(SimpleCache<String, Object> localCache,
                            RendezvousHash hrw,
                            int replicas,
                            ClusterNode self) {
        this(localCache, hrw, replicas, self, null);
    }

    public DistributedCache(SimpleCache<String, Object> localCache,
                            RendezvousHash hrw,
                            int replicas,
                            ClusterNode self,
                            String authToken) {
        this(localCache, hrw, replicas, self, authToken,
                node -> new CacheClient(node.getHost(), node.getPort(), authToken));
    }

    public DistributedCache(SimpleCache<String, Object> localCache,
                            RendezvousHash hrw,
                            int replicas,
                            ClusterNode self,
                            String authToken,
                            Function<ClusterNode, ? extends CacheRpcClient> clientFactory) {
        this.localCache = localCache;
        this.hrw = hrw;
        this.replicas = replicas;
        this.self = self;
        this.authToken = authToken;
        this.clientFactory = clientFactory != null ? clientFactory
                : node -> new CacheClient(node.getHost(), node.getPort(), authToken);
        validateTopology();
    }

    /** 拓扑校验：副本数与节点数必须合理 {@code 缺陷3修复} {@code 缺陷6修复} */
    private void validateTopology() {
        List<ClusterNode> nodes = hrw.getNodes();
        if (nodes.isEmpty()) {
            throw new CacheException("集群节点列表为空，无法初始化分布式缓存");
        }
        if (replicas < 1) {
            throw new CacheException("replicas 必须 >= 1，当前=" + replicas);
        }
        if (replicas > nodes.size()) {
            throw new CacheException(
                    "replicas=" + replicas + " 不能超过节点数=" + nodes.size()
                            + "（运维需保证各节点配置一致，否则不同节点复制范围不同会产生拓扑错乱）");
        }
        for (ClusterNode n : nodes) {
            if (n.getWeight() <= 0) {
                throw new CacheException("节点 " + n.nodeKey() + " 权重必须 > 0，当前=" + n.getWeight());
            }
        }
    }

    public SimpleCache<String, Object> getLocalCache() {
        return localCache;
    }

    public ClusterNode getSelf() {
        return self;
    }

    public RendezvousHash getHash() {
        return hrw;
    }

    public int getReplicas() {
        return replicas;
    }

    // ==================== 集群管理面集成 ====================

    /** 注入动态成员管理（可选）。注入后路由将跳过 DEAD 节点 */
    public void setMembership(ClusterMembership membership) {
        this.membership = membership;
    }

    public ClusterMembership getMembership() {
        return membership;
    }

    public void setFailoverManager(FailoverManager failoverManager) {
        this.failoverManager = failoverManager;
    }

    public FailoverManager getFailoverManager() {
        return failoverManager;
    }

    public void setRebalanceManager(RebalanceManager rebalanceManager) {
        this.rebalanceManager = rebalanceManager;
    }

    public RebalanceManager getRebalanceManager() {
        return rebalanceManager;
    }

    /**
     * 应用 membership 当前视图：用 alive 节点重建 HRW，并保留 self。
     * <p>调用时机：topologyVersion 变化后由 RebalanceManager 或管理面触发。
     * 不清理客户端连接池：DEAD 节点的连接会在 forwardWrite 时自然失败并触发重连/重试。
     */
    public synchronized void applyTopologyFromMembership() {
        if (membership == null) return;
        List<ClusterNode> alive = membership.getAliveNodes();
        if (alive.isEmpty()) {
            log.warn("applyTopologyFromMembership: alive 节点列表为空，保留当前 HRW");
            return;
        }
        if (alive.size() < replicas) {
            log.warn("applyTopologyFromMembership: alive 节点数 {} 小于 replicas {}，HRW 仍按 alive 列表重建",
                    alive.size(), replicas);
        }
        this.hrw = new RendezvousHash(alive);
        // self 视图更新
        ClusterNode selfView = membership.getSelf();
        if (selfView != null) this.self = selfView;
        log.info("拓扑已从 membership 重建: aliveNodes={}, replicas={}, self={}, topologyVersion={}",
                alive.size(), replicas, self, membership.getTopologyVersion());
    }

    /**
     * 选择 key 的 owner 列表：基于 HRW 排序，过滤掉 membership 中标记为 DEAD 的节点。
     * <p>无 membership 时直接返回 HRW 排序结果（保持向后兼容）。
     * <p>过滤后若结果为空（极端情况：所有 owner 都 DEAD），回退到原 HRW 列表，
     * 让上层调用尝试并最终失败抛 CacheException —— 这比返回空列表导致 NPE 更可控。
     */
    private List<ClusterNode> selectOwners(String key) {
        List<ClusterNode> owners = hrw.selectNodes(key, replicas);
        if (membership == null || owners.isEmpty()) {
            return owners;
        }
        List<ClusterNode> filtered = new ArrayList<>(owners.size());
        for (ClusterNode n : owners) {
            ClusterNode view = membership.getNode(n.nodeKey());
            // membership 中无此节点或状态非 DEAD 都视为可路由
            if (view == null || view.getStatus() != ClusterNode.Status.DEAD) {
                filtered.add(n);
            }
        }
        if (filtered.isEmpty()) {
            log.warn("selectOwners: key={} 所有 owner 都 DEAD，回退到原 HRW 列表", key);
            return owners;
        }
        return filtered;
    }

    /** 读策略：控制 fallback 到备份节点的行为 {@code 缺陷2修复} {@code 缺陷12修复} */
    public enum ReadPolicy {
        /** 仅读主，主失败不 fallback（吞吐降低但不读陈旧） */
        PRIMARY_ONLY,
        /** 主优先，失败时 fallback 到备（默认行为；可能读到陈旧数据） */
        FAVOR_PRIMARY
    }

    public DistributedCache withReadPolicy(ReadPolicy policy) {
        if (policy != null) readPolicyTL.set(policy);
        return this;
    }

    public ReadPolicy currentReadPolicy() {
        return readPolicyTL.get();
    }

    @FunctionalInterface
    private interface RemoteReader<T> {
        T read(ClusterNode node) throws Exception;
    }

    @FunctionalInterface
    private interface LocalReader<T> {
        T read();
    }

    @Override
    public void put(String key, Object value) {
        putInternal(key, value, 0);
    }

    @Override
    public void put(String key, Object value, long ttlMillis) {
        putInternal(key, value, ttlMillis);
    }

    private void putInternal(String key, Object value, long ttlMillis) {
        List<ClusterNode> owners = selectOwners(key);
        if (owners.isEmpty()) {
            if (ttlMillis > 0) localCache.put(key, value, ttlMillis);
            else localCache.put(key, value);
            return;
        }
        ClusterNode primary = owners.getFirst();
        byte opCode = ttlMillis > 0 ? CacheProtocol.OP_PUT_EXPIRED : CacheProtocol.OP_PUT;
        byte[] args = ttlMillis > 0
                ? CacheProtocol.encodeArgs(opCode, key, value, ttlMillis)
                : CacheProtocol.encodeArgs(opCode, key, value);
        if (primary.isSelf()) {
            if (ttlMillis > 0) localCache.put(key, value, ttlMillis);
            else localCache.put(key, value);
            replicateToBackups(key, owners, opCode, args);
            bumpLocalVersion(key);
        } else {
            forwardWrite(primary, opCode, args);
            bumpLocalVersion(key);
        }
    }

    @Override
    public Object get(String key) {
        List<ClusterNode> owners = selectOwners(key);
        if (owners.isEmpty()) {
            return localCache.get(key);
        }
        ReadPolicy policy = readPolicyTL.get();
        // 按优先级尝试：主 → 备1 → 备2 ... (PRIMARY_ONLY 在第一次失败后即返回 null)
        boolean primaryFailed = false;
        for (int i = 0; i < owners.size(); i++) {
            ClusterNode node = owners.get(i);
            if (node.isSelf()) {
                return localCache.get(key);
            }
            try {
                Object v = remoteRead(node, CacheProtocol.OP_GET,
                        CacheProtocol.encodeArgs(CacheProtocol.OP_GET, key));
                if (primaryFailed) {
                    log.warn("从备份节点 {} 读取 key={}，数据可能与主节点不一致（policy={}）",
                            node.nodeKey(), key, policy);
                }
                return v;
            } catch (Exception e) {
                primaryFailed = true;
                log.warn("从 {} 读取失败，尝试下一个 (policy={})", node.nodeKey(), policy);
                if (policy == ReadPolicy.PRIMARY_ONLY) {
                    return null;
                }
            }
        }
        return null;
    }

    /**
     * 分布式版的 getWithLoader：委托到 primary 节点的本地 getWithLoader。
     * <p>仅当本节点为 primary 时使用本地互斥锁；否则转发到 primary 节点执行（依赖远端 SimpleCache 的互斥锁）。
     */
    @Override
    public Object getWithLoader(String key, java.util.function.Supplier<Object> loader, long ttlMillis) {
        List<ClusterNode> owners = selectOwners(key);
        if (owners.isEmpty()) {
            return localCache.getWithLoader(key, loader, ttlMillis);
        }
        ClusterNode primary = owners.getFirst();
        if (primary.isSelf()) {
            return localCache.getWithLoader(key, loader, ttlMillis);
        }
        // 远端 primary：先尝试读，未命中则执行 loader 并通过 OP_PUT 写回 primary
        Object value = get(key);
        if (value != null) {
            return value;
        }
        // 缓存未命中：在本地执行 loader（避免跨节点序列化 loader），再写回 primary
        value = loader.get();
        if (value != null) {
            put(key, value, ttlMillis);
        }
        return value;
    }

    @Override
    public void remove(String key) {
        List<ClusterNode> owners = selectOwners(key);
        byte[] args = CacheProtocol.encodeArgs(CacheProtocol.OP_REMOVE, key);
        if (owners.isEmpty()) { localCache.remove(key); return; }
        ClusterNode primary = owners.getFirst();
        if (primary.isSelf()) {
            localCache.remove(key);
            replicateWrite(owners, CacheProtocol.OP_REMOVE, args);
            bumpLocalVersion(key);
        } else {
            forwardWrite(primary, CacheProtocol.OP_REMOVE, args);
            bumpLocalVersion(key);
        }
    }

    /**
     * owner-based clear：每个节点只清掉自己作为 owner 的 key，最后清理本地 owner 集合。
     * 不再广播整库 OP_CLEAR，避免清掉本节点上不属于自己 owner 集合的非主备份数据。
     * {@code 缺陷8修复}
     */
    @Override
    public void clear() {
        List<ClusterNode> nodes = hrw.getNodes();
        List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
        // JDK 21 虚拟线程：并行远程调用无需池化，每个任务独占虚拟线程
        java.util.concurrent.ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        try {
            // 向所有其他节点发起 OP_CLEAR_OWNED_ONLY，让其仅清掉自己 owner 集合
            for (ClusterNode node : nodes) {
                if (node.isSelf()) continue;
                futures.add(executor.submit(() -> {
                    try {
                        getOrCreatePrimaryClient(node).send(CacheProtocol.FLAG_NORMAL,
                                CacheProtocol.OP_CLEAR_OWNED_ONLY,
                                new byte[]{CacheProtocol.OP_CLEAR_OWNED_ONLY}, "owner-clear");
                    } catch (IOException e) {
                        log.warn("远程 owner-clear {} 失败", node.nodeKey(), e);
                    }
                    return null;
                }));
            }
            for (java.util.concurrent.Future<?> f : futures) {
                try { f.get(5, TimeUnit.SECONDS); } catch (Exception ignored) { }
            }
            // 清理本地 owner 集合
            clearOwnedLocally();
        } finally {
            executor.shutdownNow();
        }
    }

    /** 遍历本地缓存，只移除 self 在 owners 内的 key（包括作为 primary 或 backup） {@code 缺陷8修复} */
    private void clearOwnedLocally() {
        Set<String> snapshot = localCache.keys();
        for (String k : snapshot) {
            List<ClusterNode> owners = selectOwners(k);
            for (ClusterNode o : owners) {
                if (o.isSelf()) {
                    localCache.remove(k);
                    break;
                }
            }
        }
    }

    /**
     * 收到 OP_CLEAR_OWNED_ONLY 远程命令时被 CacheServer 调用，与本地 clear 走同样的 owner 过滤逻辑
     * {@code 缺陷8修复}
     */
    public void clearOwnedLocallyForRemote() {
        clearOwnedLocally();
    }

    @Override
    public int size() {
        return localCache.size();
    }

    @Override
    public Set<String> keys() {
        return localCache.keys();
    }

    // ==================== Hash 操作 ====================

    @Override
    public void hset(String key, String field, Object value) {
        List<ClusterNode> owners = selectOwners(key);
        byte[] args = CacheProtocol.encodeArgs(CacheProtocol.OP_HSET, key, field, value);
        if (owners.isEmpty()) { localCache.hset(key, field, value); return; }
        ClusterNode primary = owners.getFirst();
        if (primary.isSelf()) {
            localCache.hset(key, field, value);
            replicateWrite(owners, CacheProtocol.OP_HSET, args);
            bumpLocalVersion(key);
        } else {
            forwardWrite(primary, CacheProtocol.OP_HSET, args);
            bumpLocalVersion(key);
        }
    }

    @Override
    public Object hget(String key, String field) {
        return readFromOwners(key, () -> localCache.hget(key, field),
                node -> remoteRead(node, CacheProtocol.OP_HGET,
                        CacheProtocol.encodeArgs(CacheProtocol.OP_HGET, key, field)));
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<Object> hmget(String key, String... fields) {
        return (List<Object>) readFromOwners(key, () -> localCache.hmget(key, fields),
                node -> remoteRead(node, CacheProtocol.OP_HMGET,
                        CacheProtocol.encodeArgs(CacheProtocol.OP_HMGET, key, fields)));
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> hgetAll(String key) {
        return (Map<String, Object>) readFromOwners(key, () -> localCache.hgetAll(key),
                node -> remoteRead(node, CacheProtocol.OP_HGETALL,
                        CacheProtocol.encodeArgs(CacheProtocol.OP_HGETALL, key)));
    }

    // ==================== List 操作 ====================

    @Override
    public void lpush(String key, Object... values) {
        byte[] args = CacheProtocol.encodeArgs(CacheProtocol.OP_LPUSH, key, values);
        writeViaHrw(key, CacheProtocol.OP_LPUSH, args, () -> localCache.lpush(key, values));
    }

    @Override
    public void rpush(String key, Object... values) {
        byte[] args = CacheProtocol.encodeArgs(CacheProtocol.OP_RPUSH, key, values);
        writeViaHrw(key, CacheProtocol.OP_RPUSH, args, () -> localCache.rpush(key, values));
    }

    @Override
    public Object lpop(String key) {
        return popViaHrw(key, CacheProtocol.OP_LPOP, () -> localCache.lpop(key));
    }

    @Override
    public Object rpop(String key) {
        return popViaHrw(key, CacheProtocol.OP_RPOP, () -> localCache.rpop(key));
    }

    // ==================== Set 操作 ====================

    @Override
    public void sadd(String key, Object... members) {
        byte[] args = CacheProtocol.encodeArgs(CacheProtocol.OP_SADD, key, members);
        writeViaHrw(key, CacheProtocol.OP_SADD, args, () -> localCache.sadd(key, members));
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<Object> smembers(String key) {
        return (Set<Object>) readFromOwners(key, () -> localCache.smembers(key),
                node -> remoteRead(node, CacheProtocol.OP_SMEMBERS,
                        CacheProtocol.encodeArgs(CacheProtocol.OP_SMEMBERS, key)));
    }

    @Override
    public void srem(String key, Object... members) {
        byte[] args = CacheProtocol.encodeArgs(CacheProtocol.OP_SREM, key, members);
        writeViaHrw(key, CacheProtocol.OP_SREM, args, () -> localCache.srem(key, members));
    }

    // ==================== Sorted Set 操作 ====================

    @Override
    public void zadd(String key, double score, Object member) {
        byte[] args = CacheProtocol.encodeArgs(CacheProtocol.OP_ZADD, key, score, member);
        writeViaHrw(key, CacheProtocol.OP_ZADD, args, () -> localCache.zadd(key, score, member));
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<Object> zrange(String key, long start, long stop) {
        return (List<Object>) readFromOwners(key, () -> localCache.zrange(key, start, stop),
                node -> remoteRead(node, CacheProtocol.OP_ZRANGE,
                        CacheProtocol.encodeArgs(CacheProtocol.OP_ZRANGE, key, start, stop)));
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<Object> zrangeByScore(String key, double min, double max) {
        return (List<Object>) readFromOwners(key, () -> localCache.zrangeByScore(key, min, max),
                node -> remoteRead(node, CacheProtocol.OP_ZRANGEBYSCORE,
                        CacheProtocol.encodeArgs(CacheProtocol.OP_ZRANGEBYSCORE, key, min, max)));
    }

    // ==================== 事务（仅本地生效；跨节点无原子性保证） ====================

    /**
     * 开启事务。注意：分布式模式下事务仅在本地节点原子执行，跨节点的写操作不参与事务。
     * <p>跨节点场景下 MULTI/EXEC 无法保证原子性，应用层不应在涉及跨节点 key 的场景使用事务。
     */
    @Override
    public void multi() {
        log.warn("DistributedCache.multi() 仅在本地节点原子执行，跨节点事务无原子性保证");
        localCache.multi();
    }

    @Override
    public List<Object> exec() { return localCache.exec(); }
    @Override
    public void discard() { localCache.discard(); }

    /**
     * 监视指定键。注意：仅能检测本进程内的写修改，跨节点的写无法通过 WATCH 感知。
     */
    @Override
    public void watch(String... keys) { localCache.watch(keys); }

    // ==================== Stream 分布式路由 ====================

    /**
     * 判断 opCode 是否为 stream 写操作（用于 CacheServer.isWriteOp 路由判断）。
     */
    public static boolean isStreamWriteOp(byte opCode) {
        return switch (opCode) {
            case CacheProtocol.OP_XADD,
                 CacheProtocol.OP_XDEL,
                 CacheProtocol.OP_XTRIM,
                 CacheProtocol.OP_XGROUP_CREATE,
                 CacheProtocol.OP_XGROUP_DESTROY,
                 CacheProtocol.OP_XGROUP_CREATECONSUMER,
                 CacheProtocol.OP_XGROUP_DELCONSUMER,
                 CacheProtocol.OP_XACK,
                 CacheProtocol.OP_XCLAIM,
                 CacheProtocol.OP_XAUTOCLAIM -> true;
            default -> false;
        };
    }

    /**
     * 判断 opCode 是否为 stream 读操作（含阻塞 XREAD/XREADGROUP，需读转发到 primary）。
     */
    public static boolean isStreamReadOp(byte opCode) {
        return switch (opCode) {
            case CacheProtocol.OP_XLEN,
                 CacheProtocol.OP_XRANGE,
                 CacheProtocol.OP_XREVRANGE,
                 CacheProtocol.OP_XINFO,
                 CacheProtocol.OP_XREAD,
                 CacheProtocol.OP_XREADGROUP,
                 CacheProtocol.OP_XPENDING,
                 CacheProtocol.OP_XINFO_GROUPS,
                 CacheProtocol.OP_XINFO_CONSUMERS -> true;
            default -> false;
        };
    }

    /**
     * Stream 写操作路由：按 stream key 走 HRW 选主 → 本地执行或转发到 primary。
     *
     * <p>语义：所有 stream 写必须路由到同一 primary 节点，保证 ID 单调、PEL 一致。
     * 本节点为 primary 时本地执行并异步复制到 backup；否则转发到 primary 并返回其结果。
     *
     * <p>注意（对齐 project_memory 约束）：跨节点的 stream 写是"本地原子"的——
     * primary 本地执行成功即返回，backup 复制是异步 backlog 模式，复制失败不回滚 primary。
     * 这与现有 put/hset 等写语义一致。
     *
     * @return 写操作结果（如 XADD 返回 StreamId，XACK 返回 Long）
     */
    public Object routeStreamWrite(byte opCode, byte[] payload) {
        String key = extractKey(payload);
        if (key == null) {
            throw new CacheException("stream 写操作无法提取 key, opCode=" + Integer.toHexString(opCode & 0xFF));
        }
        List<ClusterNode> owners = selectOwners(key);
        if (owners.isEmpty()) {
            return CacheProtocol.executeCommand(localCache, opCode, payload);
        }
        ClusterNode primary = owners.getFirst();
        if (primary.isSelf()) {
            // 本地执行 + 异步复制到 backup
            Object result = CacheProtocol.executeCommand(localCache, opCode, payload);
            replicateWrite(owners, opCode, payload);
            return result;
        }
        // 转发到 primary 并返回结果
        return forwardWriteReturn(primary, opCode, payload);
    }

    /**
     * Stream 读操作路由：按 stream key 走 HRW 选主 → 本地读或转发到 primary。
     *
     * <p>语义：阻塞 XREAD / XREADGROUP 必须在 primary 上阻塞，否则会读到陈旧数据
     * 或在 backup 上阻塞却永远等不到 primary 的 XADD 唤醒。
     * 因此 stream 读不支持 backup fallback（与 KV get 不同），仅读 primary。
     *
     * @return 读操作结果（如 XRANGE 返回 List<StreamEntry>）
     */
    public Object routeStreamRead(byte opCode, byte[] payload) {
        String key = extractKey(payload);
        if (key == null) {
            throw new CacheException("stream 读操作无法提取 key, opCode=" + Integer.toHexString(opCode & 0xFF));
        }
        List<ClusterNode> owners = selectOwners(key);
        if (owners.isEmpty()) {
            return CacheProtocol.executeCommand(localCache, opCode, payload);
        }
        ClusterNode primary = owners.getFirst();
        if (primary.isSelf()) {
            return CacheProtocol.executeCommand(localCache, opCode, payload);
        }
        // 转发到 primary 读取（阻塞 XREAD 会在 primary 节点的工作线程上阻塞）
        try {
            byte[] resp = getOrCreatePrimaryClient(primary)
                    .send(CacheProtocol.FLAG_NORMAL, opCode, payload, "转发 stream 读");
            return Codec.decodeValue(resp);
        } catch (IOException e) {
            throw new CacheException("转发 stream 读到 " + primary.nodeKey() + " 失败", e);
        }
    }

    // ==================== 远程写入处理（由 CacheServer 调用） ====================

    /**
     * 处理来自远程节点的写命令（当前节点即为 HRW 选出的主节点）。
     * <p>返回命令本体执行后的结果，便于 CacheServer 把结果回传给客户端。
     * <p>修复 #14：原实现丢弃 executeCommand 返回值，导致跨节点 lpop/rpop 永远返回 null。
     * <p>修复 #4：lpop/rpop 在 pop+状态同步期间持有 per-key 锁，避免并发竞态丢失元素。
     */
    public Object handleRemoteWrite(byte opCode, byte[] payload) {
        String key = extractKey(payload);
        boolean isPop = opCode == CacheProtocol.OP_LPOP || opCode == CacheProtocol.OP_RPOP;

        // 本地执行；pop 操作与后续状态同步合并到 per-key lock 内，确保 pop + snapshot + replicate 串行化
        Object result;
        if (isPop && key != null) {
            ReentrantLock lock = lockFor(key);
            lock.lock();
            try {
                result = CacheProtocol.executeCommand(localCache, opCode, payload);
                List<ClusterNode> owners = selectOwners(key);
                replicateListState(key, owners);
            } finally {
                lock.unlock();
            }
        } else {
            result = CacheProtocol.executeCommand(localCache, opCode, payload);
            if (key != null) {
                List<ClusterNode> owners = selectOwners(key);
                for (int i = 1; i < owners.size(); i++) {
                    ClusterNode backup = owners.get(i);
                    if (backup.isSelf()) continue;
                    replicateToNode(backup, CacheProtocol.FLAG_REPLICA, opCode, payload);
                }
            }
        }
        return result;
    }

    // ==================== 内部辅助 ====================

    /**
     * lpop/rpop 专用路由：本地执行后，用 OP_LIST_REPLACE 同步剩余列表状态到备份节点（在 per-key lock 内）。
     * <p>修复 #1（异步丢包）：状态同步走 backlog，节点恢复后回放。
     * <p>修复 #4：pop + snapshot + replicate 全程持锁，避免并发竞态。
     */
    private Object popViaHrw(String key, byte opCode, java.util.function.Supplier<Object> localPop) {
        List<ClusterNode> owners = selectOwners(key);
        if (owners.isEmpty()) return localPop.get();
        ClusterNode primary = owners.getFirst();
        if (primary.isSelf()) {
            ReentrantLock lock = lockFor(key);
            lock.lock();
            try {
                Object result = localPop.get();
                replicateListState(key, owners);
                return result;
            } finally {
                lock.unlock();
            }
        }
        byte[] args = CacheProtocol.encodeArgs(opCode, key);
        return forwardWriteReturn(primary, opCode, args);
    }

    /**
     * 将指定 key 的完整列表状态同步到备份节点（使用 OP_LIST_REPLACE，保留 TTL）。
     * {@code 缺陷15修复} 传递剩余 TTL，避免备份 listReplace 后 TTL=0 永驻。
     */
    @SuppressWarnings("unchecked")
    private void replicateListState(String key, List<ClusterNode> owners) {
        Object val = localCache.get(key);
        List<Object> items;
        if (val instanceof java.util.Collection<?> c) {
            items = new ArrayList<>(c.size());
            for (Object x : c) items.add(x);
        } else {
            items = Collections.emptyList();
        }
        long remainingTtl = localCache.remainingTtlMillis(key);
        if (remainingTtl == -1) {
            remainingTtl = Long.MAX_VALUE;
        }
        // items 作为 List<Object>，remainingTtl 作为 Long。
        // CacheProtocol.executeCommand 的 OP_LIST_REPLACE 解码顺序：key (string), items (list), ttl (long)
        byte[] args = CacheProtocol.encodeArgs(CacheProtocol.OP_LIST_REPLACE, key, items, remainingTtl);
        for (int i = 1; i < owners.size(); i++) {
            ClusterNode backup = owners.get(i);
            if (backup.isSelf()) continue;
            replicateToNode(backup, CacheProtocol.FLAG_REPLICA, CacheProtocol.OP_LIST_REPLACE, args);
        }
    }

    private void writeViaHrw(String key, byte opCode, byte[] args, Runnable localWrite) {
        List<ClusterNode> owners = selectOwners(key);
        if (owners.isEmpty()) { localWrite.run(); bumpLocalVersion(key); return; }
        ClusterNode primary = owners.getFirst();
        if (primary.isSelf()) {
            localWrite.run();
            replicateWrite(owners, opCode, args);
            bumpLocalVersion(key);
        } else {
            forwardWrite(primary, opCode, args);
            bumpLocalVersion(key);
        }
    }

    private void forwardWrite(ClusterNode primary, byte opCode, byte[] args) {
        CacheRpcClient client = getOrCreatePrimaryClient(primary);
        try {
            client.send(CacheProtocol.FLAG_NORMAL, opCode, args, "转发写入");
        } catch (IOException e) {
            throw new CacheException("转发写入到 " + primary.nodeKey() + " 失败", e);
        }
    }

    private Object forwardWriteReturn(ClusterNode primary, byte opCode, byte[] args) {
        CacheRpcClient client = getOrCreatePrimaryClient(primary);
        try {
            byte[] resp = client.send(CacheProtocol.FLAG_NORMAL, opCode, args, "转发写入");
            return Codec.decodeValue(resp);
        } catch (IOException e) {
            throw new CacheException("转发写入到 " + primary.nodeKey() + " 失败", e);
        }
    }

    private void replicateWrite(List<ClusterNode> owners, byte opCode, byte[] args) {
        for (int i = 1; i < owners.size(); i++) {
            ClusterNode backup = owners.get(i);
            if (backup.isSelf()) continue;
            replicateToNode(backup, CacheProtocol.FLAG_REPLICA, opCode, args);
        }
    }

    private void replicateToBackups(String key, List<ClusterNode> owners, byte opCode, byte[] args) {
        for (int i = 1; i < owners.size(); i++) {
            ClusterNode backup = owners.get(i);
            if (backup.isSelf()) continue;
            replicateToNode(backup, CacheProtocol.FLAG_REPLICA, opCode, args);
        }
    }

    /**
     * 复制到备份节点：fire-and-forget 但带 backlog 补偿。
     * <p>修复 #1：节点不健康或发送失败时把 (opCode, args) 加入对应备份节点的 backlog；
     * 健康检查恢复节点时先回放 backlog，再清 unhealthy 标记。
     */
    private void replicateToNode(ClusterNode node, byte flags, byte opCode, byte[] args) {
        if (unhealthyNodes.contains(node.nodeKey())) {
            enqueueBacklog(node.nodeKey(), opCode, args);
            logReplicaFailure(node, "节点不健康，已入 backlog 待恢复回放");
            return;
        }
        CacheRpcClient client = getOrCreateReplicaClient(node);
        try {
            client.sendAsync(flags, opCode, args, "复制");
            lastReplicaWarnMs.remove(node.nodeKey());
        } catch (IOException e) {
            unhealthyNodes.add(node.nodeKey());
            enqueueBacklog(node.nodeKey(), opCode, args);
            logReplicaFailure(node, "复制失败已入 backlog: " + e.getMessage());
        }
    }

    private void enqueueBacklog(String nodeKey, byte opCode, byte[] args) {
        ConcurrentLinkedQueue<PendingReplica> q =
                replicationBacklog.computeIfAbsent(nodeKey, k -> new ConcurrentLinkedQueue<>());
        if (q.size() >= BACKLOG_MAX_PER_NODE) {
            // 队列已满：尝试淘汰最旧的命令腾出空间（丢弃最旧而非最新，保留最近状态）
            PendingReplica evicted = q.poll();
            if (evicted != null) {
                log.warn("节点 {} backlog 已满 ({})，淘汰最旧命令以容纳最新 (evicted op={}, new op={})",
                        nodeKey, BACKLOG_MAX_PER_NODE,
                        Integer.toHexString(evicted.opCode() & 0xFF),
                        Integer.toHexString(opCode & 0xFF));
            }
        }
        q.offer(new PendingReplica(opCode, args));
    }

    /** 获取各节点 backlog 当前长度，供运维监控 */
    public Map<String, Integer> getBacklogSizes() {
        Map<String, Integer> sizes = new java.util.HashMap<>();
        for (Map.Entry<String, ConcurrentLinkedQueue<PendingReplica>> e : replicationBacklog.entrySet()) {
            sizes.put(e.getKey(), e.getValue().size());
        }
        return sizes;
    }

    /** backlog 中保留的待重放复制命令 {@code 缺陷1修复} */
    private record PendingReplica(byte opCode, byte[] args) {}

    /** 读路径：按 owners 顺序，主失败后根据策略决定是否 fallback {@code 缺陷2修复} {@code 缺陷12修复} */
    private Object readFromOwners(String key,
                                   LocalReader<Object> localRead,
                                   RemoteReader<Object> remoteRead) {
        List<ClusterNode> owners = selectOwners(key);
        if (owners.isEmpty()) return localRead.read();
        ReadPolicy policy = readPolicyTL.get();
        boolean primaryFailed = false;
        for (ClusterNode node : owners) {
            if (node.isSelf()) return localRead.read();
            try {
                Object v = remoteRead.read(node);
                if (primaryFailed) {
                    log.warn("从备份节点 {} 读取 key={}，可能陈旧 (policy={})",
                            node.nodeKey(), key, policy);
                }
                return v;
            } catch (Exception e) {
                primaryFailed = true;
                log.warn("从 {} 读取失败，尝试下一个 (policy={})", node.nodeKey(), policy);
                if (policy == ReadPolicy.PRIMARY_ONLY) {
                    return null;
                }
            }
        }
        return null;
    }

    private Object remoteRead(ClusterNode node, byte opCode, byte[] args) throws IOException {
        byte[] resp = getOrCreatePrimaryClient(node).send(CacheProtocol.FLAG_NORMAL, opCode, args, "转发读取");
        return Codec.decodeValue(resp);
    }

    /**
     * 复用客户端连接：PRIMARY 池与 REPLICA 池分离，避免同步响应与异步 fire-and-forget 在同一连接上交错
     * {@code 缺陷5修复}
     */
    private CacheRpcClient getOrCreatePrimaryClient(ClusterNode node) {
        return primaryClientPool.computeIfAbsent(node.nodeKey(),
                k -> clientFactory.apply(node));
    }

    private CacheRpcClient getOrCreateReplicaClient(ClusterNode node) {
        return replicaClientPool.computeIfAbsent(node.nodeKey(),
                k -> clientFactory.apply(node));
    }

    /** 每 30 秒最多告警一次，避免单节点运行时日志刷屏 */
    private void logReplicaFailure(ClusterNode node, String detail) {
        long now = System.currentTimeMillis();
        Long last = lastReplicaWarnMs.get(node.nodeKey());
        if (last == null || now - last > 30_000) {
            log.warn("备节点复制失败 {}: {}", node.nodeKey(), detail);
            lastReplicaWarnMs.put(node.nodeKey(), now);
        }
    }

    /**
     * 从 payload 中提取 key（第一个编码值，opCode 之后的下一个字段）。
     */
    private static String extractKey(byte[] payload) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
            in.readByte();
            byte type = in.readByte();
            if (type == Codec.TYPE_STRING) {
                int len = in.readUnsignedShort();
                byte[] utf = new byte[len];
                in.readFully(utf);
                return new String(utf, java.nio.charset.StandardCharsets.UTF_8);
            }
        } catch (Exception ignored) {}
        return null;
    }

    /**
     * 启动节点健康检测：每 10 秒重连不健康节点，连接成功后先回放 backlog 再清不健康标记。
     * {@code 缺陷1修复}
     */
    public void startHealthCheck() {
        healthChecker = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "cache-health");
            t.setDaemon(true);
            return t;
        });
        healthChecker.scheduleAtFixedRate(() -> {
            for (String nodeKey : new ArrayList<>(unhealthyNodes)) {
                if (reconnectAndDrainBacklog(nodeKey)) {
                    unhealthyNodes.remove(nodeKey);
                    log.info("节点 {} 已恢复健康并完成 backlog 回放", nodeKey);
                }
            }
        }, 10, 10, TimeUnit.SECONDS);
    }

    /** 重建连接 → 健康探测 → 回放 backlog。任一步失败则保留 unhealthy 标记 */
    private boolean reconnectAndDrainBacklog(String nodeKey) {
        ClusterNode node = findNodeByKey(nodeKey);
        if (node == null) {
            log.debug("节点 {} 不在拓扑中，跳过", nodeKey);
            return false;
        }
        try {
            // 替换 replica 池连接（PRIMARY 客户端若也断会自然重连，这里只处理 replica 通道）
            CacheRpcClient old = replicaClientPool.remove(nodeKey);
            if (old != null) old.close();
            CacheRpcClient client = clientFactory.apply(node);
            client.send(CacheProtocol.FLAG_NORMAL, CacheProtocol.OP_SIZE,
                    new byte[]{CacheProtocol.OP_SIZE}, "健康检查");
            replicaClientPool.put(nodeKey, client);

            // 先回放 backlog：所有 backlog 命令通过新 client 异步发送
            ConcurrentLinkedQueue<PendingReplica> q = replicationBacklog.get(nodeKey);
            if (q != null && !q.isEmpty()) {
                PendingReplica p;
                int drained = 0;
                while ((p = q.poll()) != null) {
                    try {
                        client.sendAsync(CacheProtocol.FLAG_REPLICA, p.opCode(), p.args(), "backlog回放");
                        drained++;
                    } catch (IOException e) {
                        // 回放失败：放回队列头部，下次再试
                        ConcurrentLinkedQueue<PendingReplica> qq =
                                replicationBacklog.computeIfAbsent(nodeKey, k -> new ConcurrentLinkedQueue<>());
                        qq.offer(p);
                        log.warn("节点 {} backlog 回放中断，剩余 {} 条将下次重试", nodeKey, qq.size());
                        return false;
                    }
                }
                log.info("节点 {} backlog 回放完成 {} 条", nodeKey, drained);
                replicationBacklog.remove(nodeKey);
            }
            return true;
        } catch (Exception e) {
            log.debug("节点 {} 仍不可达: {}", nodeKey, e.getMessage());
            return false;
        }
    }

    /**
     * 清除 backlog（用于停机或运维）。被 shutdown 调用。
     */
    public void clearBacklog() {
        replicationBacklog.clear();
    }

    /** 拓扑中所有节点 {@code 缺陷11修复} 提供公开 accessor 以便运维可观测 */
    public List<ClusterNode> getTopology() {
        return hrw.getNodes();
    }

    /**
     * 重新加载拓扑（受控场景，需手动手动触发）。
     * <p>注意：该操作会替换 HRW 哈希环。正在进行的请求可能持有旧 owners 列表，存在短暂路由不一致窗口，
     * 建议仅在低峰期或 maintenance window 调用。 {@code 缺陷11修复}
     */
    public synchronized void reloadTopology(List<ClusterNode> newNodes, int newReplicas, ClusterNode newSelf) {
        if (newNodes == null || newNodes.isEmpty()) {
            throw new CacheException("reloadTopology: 节点列表不能为空");
        }
        if (newReplicas < 1 || newReplicas > newNodes.size()) {
            throw new CacheException("reloadTopology: 非法 replicas=" + newReplicas + " nodes=" + newNodes.size());
        }
        for (ClusterNode n : newNodes) {
            if (n.getWeight() <= 0) {
                throw new CacheException("节点 " + n.nodeKey() + " 权重必须 > 0");
            }
        }
        closeAllClients();
        unhealthyNodes.clear();
        replicationBacklog.clear();
        this.hrw = new RendezvousHash(newNodes);
        this.replicas = newReplicas;
        if (newSelf != null) this.self = newSelf;
        log.info("拓扑已重新加载: nodes={}, replicas={}, self={}", newNodes, newReplicas, self);
    }

    private void closeAllClients() {
        for (CacheRpcClient c : primaryClientPool.values()) c.close();
        for (CacheRpcClient c : replicaClientPool.values()) c.close();
        primaryClientPool.clear();
        replicaClientPool.clear();
    }

    private ClusterNode findNodeByKey(String nodeKey) {
        for (ClusterNode node : hrw.getNodes()) {
            if (node.nodeKey().equals(nodeKey)) return node;
        }
        return null;
    }

    private ReentrantLock lockFor(String key) {
        return keyLocks.computeIfAbsent(key, k -> new ReentrantLock());
    }

    /**
     * forward 写完成后，在本地 versions 表补 bump，保证本进程 WATCH 用例可中断 {@code 缺陷13修复}。
     * <p>判断逻辑：bulkLocalVersion 应当 bump 的情形是「self 即 primary 时 SimpleCache.put 已 本次 bump 过」的补集，
     * 即本进程只是发起方但写命令实际由远端 primary 执行 — 此时本地 versions 不会因远端执行 bump 而变化，
     * 应用层同进程后续 watch+exec 会因版本号无变化而无法中断。补 bump 避免这一假设错位。
     */
    private void bumpLocalVersion(String key) {
        if (self == null) return;
        List<ClusterNode> owners = selectOwners(key);
        // 只在 self 不是 primary（写命令被转发）时补偿 bump；self 本身即 primary 时已在 SimpleCache 中 bump
        if (!owners.isEmpty() && !owners.getFirst().equals(self)) {
            localCache.bumpVersionForRemoteWrite(key);
        }
    }

    /**
     * 关闭所有客户端连接和健康检测
     */
    public void shutdown() {
        if (healthChecker != null) {
            healthChecker.shutdownNow();
        }
        closeAllClients();
    }
}
