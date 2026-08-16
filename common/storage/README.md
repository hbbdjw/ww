# storage 模块 —— 分布式对象存储

基于 **HRW 路由 + 主节点本地写 + 同步副本复制 + read-after-write 一致性** 的分布式对象存储，提供 S3 风格的 Bucket/Object API（挂载于 `/storage`）。

- 包名：`com.ww.storage`
- 依赖：仅依赖 `common`（`ClusterNode` / `RendezvousHash` / `ClusterProperties` / `CachePortProperties`）
- 消费方：`admin` 模块（`StorageController` 对外 API + `InternalReplicationController` 节点间内部通信）

---

## 1. 模块结构

| 类                                        | 职责                                                                               |
| ----------------------------------------- | ---------------------------------------------------------------------------------- |
| `StorageProperties`                     | 配置绑定（`storage.*`），本地磁盘、Set 划分、加密开关、端口偏移等                |
| `StorageConfig`                         | 模块装配：复用`cluster.*` 拓扑构建节点视图，组装全部组件                         |
| `Drive`                                 | 磁盘：数据落盘目录、`objects/ab/cd/<sha1>/object.{data,meta}` 布局、名称校验     |
| `DriveManager`                          | 加载`storage.drives`，按 `drivesPerSet` 划分 Set                               |
| `StorageSet`                            | Set（条带）：一组 Drive 的逻辑组合，对象写入 Set 内按 hash 选中的单个 Drive        |
| `SetLocator`                            | 稳定散列：`SHA-1(bucket + "\0" + key)` → Set 索引 → Drive                      |
| `BucketStore`                           | Bucket 元信息：`.bucket` 文件与对象共盘，冗余写入所有在线 Drive                  |
| `BucketInfo`                            | Bucket 元数据（name / createdAt / version）                                        |
| `ObjectStore`                           | 对象本地读写：`.data` + `.meta` 原子落盘、MD5 ETag、version 递增、list 扫描    |
| `ObjectMetadata`                        | 对象元数据（key / size / contentType / etag / createdAt / version / userMetadata） |
| `ObjectMetadataCache`                   | 元数据 LRU 缓存（默认 1 万条），减少 HEAD/GET 磁盘 IO                              |
| `ByteBufferPool`                        | 直接内存`ByteBuffer` 池（默认 64KB × 64），避免堆外反复分配                     |
| `CryptoCodec` / `IdentityCryptoCodec` | 加解密扩展点（当前为透传，预留 AES-GCM / SSE 接入）                                |
| `StorageNode`                           | 节点视图：host + HTTP 端口 + weight + self 标记                                    |
| `NodeRouter`                            | HRW 路由：为`(bucket,key)` 选出 primary 与 replica 列表；**支持运行时重建**（扩缩容） |
| `StorageNodeClient`                     | 节点间 HTTP 客户端：转发写/读/删、副本复制、Bucket 广播、list 拉取、**拓扑/维护态广播、本地删除、心跳探活、failover 广播、反向同步触发** |
| `DistributedStorage`                    | 分布式门面：对外提供 bucket/object 全部操作，内部完成路由与复制；**维护态标志、立即对账、原主反向同步** |
| `StorageTopologyManager`                | 运行时拓扑管理：动态 addNode/removeNode + HRW 重建 + 拓扑广播 + 持久化（扩缩容）；**节点健康状态机 + failover epoch + markDead/markAlive** |
| `StorageMigrationService`               | 数据迁移：drain/drainAndPurge/rebalanceAll + 进度持久化（崩溃续跑）            |
| `StorageFailoverManager`                | 故障检测与自动 failover：心跳探测 + 状态机（ALIVE/SUSPECT/DEAD/QUARANTINE）+ 立即对账 + 原主反向同步 + 脑裂防护 |
| `NodeStatus`                            | 节点健康状态枚举（failover 状态机）                                              |
| `ReadHandle`                            | 读取句柄抽象（本地 sendfile / 远端流）                                             |
| `StorageException`                      | 统一异常                                                                           |

**admin 模块配套组件**：

| 类                                         | 职责                                     |
| ------------------------------------------ | ---------------------------------------- |
| `StorageController`（admin）             | 对外 S3 兼容 API，`/storage/**`，**含维护态拦截** |
| `StorageClusterController`（admin）       | 集群扩缩容 + failover 管理 API，`/storage/cluster/**` |
| `InternalReplicationController`（admin） | 节点间内部端点，`/internal/storage/**`（含 `/topology` `/maintenance` `/object/local` `/ping` `/failover` `/reverse-reconcile`） |

---

## 2. 架构设计

```
                    ┌─────────────────────────────────────────────┐
 客户端  ──────────►│              DistributedStorage              │  ← 分布式门面
                    │  路由 → 转发 / 本地读写 / 复制 / list 合并    │
                    │  维护态标志（扩缩容期间拒客户端读写）          │
                    └──────┬──────────────────┬──────────────────┘
                           │                  │
              ┌────────────▼───────┐  ┌───────▼──────────────────┐
              │     NodeRouter     │  │   StorageNodeClient      │  ← 节点间 HTTP
              │   HRW 选主/副本     │  │  /internal/storage/*     │
              │   可运行时重建      │  │  拓扑/维护态广播、本地删除 │
              └────────┬───────────┘  └──────────────────────────┘
                       │ ▲ rebuild
              ┌────────│─┴────────────┐
              │ StorageTopologyManager│  ← 运行时拓扑（add/remove）+ 持久化
              │   + nodeStatus 状态机 │  + markDead/markAlive + failoverEpoch
              │ StorageMigrationService│ ← 数据迁移（drain/rebalance）+ 进度持久化
              │ StorageFailoverManager│ ← 心跳探测 + 自动 failover + 反向同步
              └────────┬──────────────┘
                       │
              ┌────────▼───────────┐
              │      ObjectStore   │      InternalReplicationController（对端）
              │  putPrimary/putReplica/get/head/delete/list │
              └──────┬─────────────┘
                     │ SetLocator（SHA-1 稳定散列）
              ┌──────▼─────────────┐
              │     DriveManager   │
              └──────┬─────────────┘
              ┌──────▼─────────────┐
              │   Drive（磁盘）      │   buckets/{bucket}/objects/ab/cd/{sha1}/object.{data,meta}
              └────────────────────┘
```

**两层路由互不耦合**：

- `NodeRouter`（HRW）决定 **哪个节点** 持有对象主副本 —— 跨节点；
- `SetLocator`（SHA-1 散列）决定 **本机哪块磁盘** 落盘 —— 单机内。

**扩缩容与读写路径解耦**：

- `StorageTopologyManager` / `StorageMigrationService` 独立于读写路径，仅在扩缩容时通过 `NodeRouter.rebuild()` 切换路由视图；
- 维护态拦截放在客户端入口 `StorageController`，内部复制与迁移端点不受限，保证迁移期间可读取源节点数据写入新主。

**failover 与扩缩容协同**：

- `StorageFailoverManager` 复用 `StorageTopologyManager` 的拓扑管理，但走不同的状态机分支：failover 自动 markDead（不进维护态，客户端无感），扩缩容手动 addNode/removeNode（进维护态，拒客户端读写）；
- `NodeRouter` 重建时仅纳入可路由节点（ALIVE/SUSPECT），DEAD/QUARANTINE 节点不路由 —— 这是 failover 的核心：primary 自动切到副本，无需显式 PROMOTE，避免脑裂双主。

---

## 3. 实现原理

### 3.1 路由：HRW（最高随机权重哈希）

`NodeRouter` 复用 common 模块的 `RendezvousHash`（与 cache 集群同一算法，经生产验证），以 `routingKey = bucket + "/" + key` 为输入，按 `weight * hash(nodeKey, key)` 评分降序选出 `replicas` 个节点：

- `[0]` = **primary（主）**：唯一执行读写；
- `[1..]` = **replica（副本）**：接收主节点复制、承担数据冗余；
- `replicas` 取值 = `cluster.replicas`（默认 2），并限制在 `1 ~ 节点数` 之间；
- `nodes.size() > 1` 时才判定为分布式（`isDistributed()`），单节点直接走本地，避免无谓的 HTTP 自调用。

### 3.2 落盘布局

```
{drive-root}/
└── buckets/
    └── {bucket}/
        ├── .bucket                    # Bucket 元信息（冗余写所有在线 Drive）
        └── objects/
            └── {sha1[0:2]}/
                └── {sha1[2:4]}/
                    └── {sha1}/        # SHA-1(bucket\0key) 全量，两级散列防目录爆炸/路径穿越
                        ├── object.data
                        └── object.meta
```

- 元数据与数据共盘，**无独立元数据库**；`object.meta` 为 JSON（`ObjectMetadata`）；
- 覆盖写通过 `.data.tmp` / `.meta.tmp` + 原子 rename 实现，**先暴露 `.meta` 再暴露 `.data`** —— 元数据可见即代表数据已完整落盘，杜绝读到半成品；
- `version`：同 `(bucket,key)` 维度单调递增（主写路径读取旧 `.meta` 的 version + 1），用于 list 合并去重与覆盖检测；
- `etag`：主写路径流式计算 MD5(明文) 作为内容指纹，副本复制沿用主节点计算的 etag/version，保证主备一致。

### 3.3 写路径（主写 + 同步复制）

```
PUT (bucket,key)
  → HRW 选 primary
  → 本节点是 primary 或单机：
       objectStore.putPrimary()       # 本地原子落盘，计算 ETag/version
       → replicateToObject()          # 并行（虚拟线程）复制到各 replica，失败入待重试队列
       → 返回元数据
  → 否则：
       nodeClient.forwardPut(primary) # 透明转发到主节点执行上述流程
```

- **主节点本地写成功即 ack**，复制失败不阻断主写，入待重试队列由后台线程周期重放（指数退避），主节点数据已持久化；
- 复制使用 `Executors.newVirtualThreadPerTaskExecutor()`（JDK 21），每个副本一个虚拟线程，IO 阻塞不占用平台线程；
- 大对象流式传输（`BodyPublishers.ofInputStream`），不全量驻留内存。

### 3.4 读路径（read-after-write 关键）

```
GET/HEAD (bucket,key)
  → HRW 选 primary
  → 仅从 primary 读（PRIMARY_ONLY 语义）：
       HEAD：优先 ObjectMetadataCache → 未命中读 .meta → 回填缓存
       GET ：objectStore.get() → 本地 FileSystemResource（Tomcat sendfile 零拷贝）或流
  → 非本节点：nodeClient.forwardGet/forwardHead 转发到主节点
```

**为什么必须只读主节点？** 写必走主、读必走主，因此"写成功 ack 之后的任何读"必然落在已落盘的主节点上 —— 天然满足 read-after-write，无需任何版本协商。

**主节点故障语义**：PRIMARY_ONLY 语义下不 fallback 副本读（避免读到陈旧副本破坏一致性）；主节点故障由 `StorageFailoverManager` 自动检测（心跳 3 次失败 → DEAD），`NodeRouter` 重建后过滤 DEAD 节点，primary 自动切到副本（见 3.12），故障期间约 15s 内该 key 不可读写。

### 3.5 删路径

```
DELETE (bucket,key)
  → HRW 选 primary → 主节点本地删（清理空目录）→ 通知各 replica 删（失败仅告警）
  → 非本节点：转发到主节点
```

### 3.6 list（全节点并行合并）

```
GET /storage/buckets/{bucket}/objects?prefix=&marker=&maxKeys=
  → 本地扫描 + 对端节点并行拉取（虚拟线程，每节点 30s 超时）
  → 按 key 合并去重：同 key 取 version 最大者
  → 全局应用 prefix / marker（字典序 >）/ maxKeys，返回 nextMarker + isTruncated
```

- 本地与对端均全量拉取不截断（`MERGE_SCAN_LIMIT = Integer.MAX_VALUE`，截断会导致大桶对象在合并时静默缺失，且 nextMarker 无法到达被截断的尾部）；
- 对端节点不可达仅告警，不阻断整体 list。

### 3.7 Bucket 语义

- 创建：本机建目录 + 写 `.bucket`，然后 **广播到所有对端节点**（best-effort，保证各节点后续 put 时 bucket 已就绪）；
- 元信息冗余写所有在线 Drive（任一 Drive 损坏仍可恢复）；
- 删除：默认要求 Bucket 为空，`recursive=true` 强制清空。

### 3.8 一致性模型

| 场景             | 保证                                                 |
| ---------------- | ---------------------------------------------------- |
| 写后读（同 key） | read-after-write：主节点本地落盘后任何读必读到最新   |
| 读一致性         | PRIMARY_ONLY，不读副本                               |
| 副本一致性       | 主节点计算的 etag/version 原样落到副本，主备字节一致 |
| 覆盖写           | version 单调递增，list 合并取最大 version            |
| 主节点故障       | 自动 failover：心跳 3 次失败后 markDead，primary 切副本（RTO≈15s）；原主恢复经反向同步再纳路由 |
| 复制失败         | 入待重试队列周期重放（指数退避），主节点数据已持久；周期对账 `reconcile()` 兜底补复制 |

### 3.9 性能优化（JDK 21 / Spring Boot 3）

| 手段           | 说明                                                                                     |
| -------------- | ---------------------------------------------------------------------------------------- |
| 直接内存缓冲池 | `ByteBufferPool` 64KB×64 堆外复用，默认写路径零堆内 byte[] 拷贝；MD5 直接消费直接内存 |
| sendfile       | 无加密 + 源为本地文件（副本复制）时`FileChannel.transferFrom` 走内核零拷贝             |
| 虚拟线程       | 副本复制、分布式 list 全部用虚拟线程并行                                                 |
| 元数据 LRU     | `ObjectMetadataCache` 默认 1 万条，HEAD/GET 命中跳过磁盘 IO；写/删时本地失效保证一致性 |

### 3.10 加密扩展点

`CryptoCodec` 接口预留（`encryptWrite` / `decryptRead` / `enabled`）。当前 `IdentityCryptoCodec` 为透传（`storage.crypto-enabled=true` 时也仅告警不加密）。后续接入 AES-GCM / SSE 时替换 Bean 即可，业务层无感知；加密与 read-after-write 正交 —— 只改落盘字节，不改读写路由与时序。

### 3.11 动态扩缩容（storage cluster scale-out / transfer / merge）

storage 模块支持运行时动态扩缩容：**添加节点**、**转移节点数据**、**合并并下线节点**，通过 HTTP API 触发，允许扩缩容期间服务暂时不可用（停服-换拓扑-迁数据-恢复）。

#### 设计取舍

| 维度 | 选择 | 原因 |
| --- | --- | --- |
| 在线 vs 停服 | 停服迁移 | 大文件对象迁移是长时 IO；在线 gossip + 增量 rebalance 对存储过度复杂 |
| 一致性 | 维护态拒读写 + read-after-write | 避免迁移期间双 primary、读陈旧 |
| 冲突解决 | LWW via version（复用 `ObjectStore.putReplica`） | 迁移幂等，重放安全 |
| 失败处理 | 进度持久化，崩溃续跑 | 大对象迁移可中断恢复 |
| 源端清理 | 迁移完成后可选清理 | 保留回滚窗口 |

#### 核心组件

- **`NodeRouter.rebuild()`**：内部状态由 `final` 改为 `volatile`，提供同步重建方法；构造新快照后原子替换引用，读路径无锁（参考 cache 模块 `applyTopologyFromMembership` 语义）。
- **`StorageTopologyManager`**：运行时维护节点视图 `Map<nodeKey, StorageNode>`，提供 `addNode` / `removeNode` / `applyRemoteTopology`，每次变更：
  1. 调用 `NodeRouter.rebuild()` 重建本地 HRW；
  2. 编码拓扑为 JSON 广播到所有对端（`/internal/storage/topology`），对端按自身 host:httpPort 重新标记 self 后重建；
  3. 持久化到 `cluster-topology.json`，重启时优先恢复（防回退到旧静态配置）。
- **`StorageMigrationService`**：负责数据搬运，三种语义：
  - `drainNode(source)`：把源节点对象复制到新 HRW 主（用于 transfer）；
  - `drainAndPurge(source)`：迁移后清理源端本地文件（用于 merge）；
  - `rebalanceAll()`：扫描所有节点，把本地不是新主的对象迁到新主（用于 scale-out，约 1/(N+1) 的 key 换主）。
  - 进度持久化到 `.migration-progress.json`，崩溃续跑跳过已完成 key；成功完成后清空进度文件。
- **维护态标志**：`DistributedStorage.maintenance`，仅作记录与查询；**拦截放在客户端入口 `StorageController`**，使内部复制端点 `/internal/storage/*` 与迁移逻辑在维护态下仍可正常工作（迁移需读取源节点数据）。

#### 一致性保证（read-after-write）

1. 维护态期间 `StorageController` 拒绝所有客户端读写（PUT/GET/HEAD/DELETE/LIST）；
2. 内部复制与迁移端点不受维护态影响，迁移可正常调用 `forwardGet` 读取源节点、`replicatePut` 写入新主；
3. 迁移完成后解除维护态，新写入走新 HRW 主，读也走新主 —— 天然 read-after-write；
4. `ObjectStore.putReplica` 的 version 幂等校验保证迁移重放安全，可并发可重入。

#### 拓扑广播

所有节点等层级，任一节点发起的拓扑变更会广播到全部对端：

```
发起节点                          对端节点
  │                                 │
  │  POST /internal/storage/topology│
  │ ─────────────────────────────► │
  │  body: [{host,httpPort,weight}]│
  │  ?replicas=N                   │
  │                                 │
  │                  applyRemoteTopology()
  │                  → 按 self host:httpPort 重新标记
  │                  → NodeRouter.rebuild()
  │                  → DistributedStorage.refreshNodeIndex()
  │ ◄────────────── 200 OK ──────── │
```

广播不含 `self` 标志，对端按自身身份重新标记，避免双向广播循环。

### 3.12 自动 failover（节点故障检测与切换）

storage 模块支持**节点故障自动 failover**：主节点故障时，其 primary 自动切到副本节点，客户端无感恢复读写（RTO ≈ 15s，RPO ≤ 副本对账延迟）。

#### 生存能力声明

> survive loss of 1 storage node while continuing read/write，RTO ≈ 15s（心跳 3 次 × 5s），RPO ≤ reconcile 延迟（failover 立即触发对账）+ 分区期间未复制的写。

#### 状态机

```
ALIVE ──(心跳连续 2 次失败)──► SUSPECT ──(再失败 1 次，累计 3 次)──► DEAD
  ▲                               │                                     │
  │◄──(心跳恢复)──────────────────┘                                     │
  │                                                                     │
  │◄──(反向同步完成)──────────── QUARANTINE ◄──(心跳恢复)──────────────┘
```

| 状态 | 纳入路由 | 语义 |
| --- | --- | --- |
| ALIVE | 是 | 健康 |
| SUSPECT | 是 | 心跳失败 1-2 次，仍路由（仅怀疑） |
| DEAD | 否 | 已确认故障，primary 已 failover 到副本 |
| QUARANTINE | 否 | 原主恢复中，反向同步补齐数据，不接路由 |

#### failover 触发（DEAD 时）

1. `StorageTopologyManager.markDead(nodeKey)` —— 路由过滤 + `failoverEpoch++`
2. 广播 failover 事件到对端（`/internal/storage/failover`），对端更新本地状态与 epoch（旧 epoch 广播被丢弃）
3. `DistributedStorage.triggerReconcileNow()` —— 立即补副本（不等 5min 周期）
4. `NodeRouter` 重建后不含 DEAD 节点，`selectPrimary` 用剩余节点 HRW 选主 —— primary 自动切到副本，无需显式 PROMOTE，避免脑裂双主

#### 原主恢复（DEAD → QUARANTINE → ALIVE）

1. 心跳恢复 → `markAlive(quarantine=true)` → QUARANTINE（不纳入路由）
2. 触发原主反向同步（`/internal/storage/reverse-reconcile`）：原主遍历本地对象，对 primary 已漂移的对象从新 primary 拉取 version 落后的数据补齐本地
3. 校验通过 → `markAlive(quarantine=false)` → ALIVE（纳入路由）
4. 超时（5min）未完成反向同步则强制转 ALIVE（避免长期隔离）

#### 脑裂防护

- **DEAD/QUARANTINE 不路由**：`NodeRouter` 重建时仅纳入 ALIVE/SUSPECT 节点，`selectPrimary` 不会选到原主
- **failoverEpoch fencing**：每次 markDead 递增 epoch，广播带 epoch；对端收到旧 epoch 广播丢弃，防止旧状态覆盖新状态
- **LWW via version 兜底**：分区期间原主可能仍有写入（无法完全防），恢复后由 version 比对丢弃旧版本（RPO > 0 的来源）

#### 静态容量

| 集群规模 | 副本数 | 容忍节点故障数 | 故障后剩余容量 |
| --- | --- | --- | --- |
| 3 节点 | 2 | 1 | 2 节点承担 3 节点流量（1.5×） |
| 5 节点 | 3 | 2 | 3 节点承担 5 节点流量（1.67×） |

> 剩余节点需预留 ~50% 容量 headroom（HA Iron Law 静态稳定性要求，不依赖故障后紧急扩容）。

---

## 4. 集群

### 4.1 拓扑配置（复用 `cluster.*`）

集群拓扑与 cache 模块共用一套 `cluster.*` 配置，storage 仅在其上补充"HTTP 端口"维度（`StorageNode`）：

```yaml
cluster:
  enabled: true
  replicas: 2                 # 副本数（含主共 2 份），取值 [1, 节点数]
  nodes:
    - host: 127.0.0.1
      port: 8701              # cache TCP 端口
      weight: 1.0
      # httpPort: 8081       # 可选：显式指定节点 HTTP 端口
cache:
  port: 8701                  # 本节点 cache TCP 端口（用于识别 self）
```

### 4.2 等层级节点（peer）

所有节点**等层级、无主从**：任一节点都可接收任一请求，非主节点按 HRW 透明转发到主节点，对客户端无感。

### 4.3 HTTP 端口推导

节点间 HTTP 通信端口（admin `server.port`）：

1. 优先 `cluster.nodes[].httpPort`（显式配置）；
2. 否则 `httpPort = cluster.nodes[].port + storage.httpPortOffset`，offset 默认 = `server.port - cache.port`（对称部署推导成立，如 8701 → 8081，offset = -620）；
3. 单机模式直接用本机 `server.port`。

### 4.4 副本与容错

- 每个对象的副本数 = `cluster.replicas`，副本节点 = HRW 评分 [1..]；
- **数据冗余在"节点间"复制**（整对象复制，非纠删码）；同一逻辑 SetIndex 在每个节点各有一份本地 Drive 子集；
- **自动 failover**（见 3.12）：主节点故障时，`StorageFailoverManager` 通过心跳探测（5s × 3 次 = 15s RTO）将节点标记 DEAD，`NodeRouter` 重建后仅纳入 ALIVE/SUSPECT 节点，primary 自动切到副本，客户端无感恢复读写；原主恢复后经 QUARANTINE 隔离 + 反向同步补齐数据再纳入路由（防脑裂）。

### 4.5 单机模式

`cluster.enabled=false` 或 nodes 为空时：

- 构造单节点 StorageNode（本机 `server.port`，weight=1.0）；
- `isDistributed()=false`：所有操作直接走本地 `ObjectStore`，无 HTTP 转发/复制；
- 仍严格遵守 read-after-write（本地原子写 + 本地主读）。

### 4.6 动态拓扑管理（扩缩容）

storage 模块在静态 `cluster.*` 配置之上叠加了运行时动态拓扑：

- **初始拓扑**：启动时由 `StorageConfig.buildStorageNodes()` 从 `cluster.*` 构建，标记 self；
- **运行时变更**：通过 `StorageTopologyManager.addNode()` / `removeNode()` 修改内存视图，调用 `NodeRouter.rebuild()` 重建 HRW；
- **持久化**：每次成功变更后写入 `{storage.root-dir}/cluster-topology.json`，重启时若文件存在且节点数 ≥ 静态配置则采用持久化版本（防回退到旧静态配置）；
- **拓扑版本号**：`topologyVersion` 每次成功变更递增，便于排查与一致性校验；
- **与 cache 拓扑解耦**：cache 的 `/cluster/join` 不会触发 storage 扩缩容，反之亦然。两套拓扑独立演进，符合现有模块边界。

**持久化文件示例**（`{storage.root-dir}/cluster-topology.json`）：

```json
{
  "version" : 3,
  "replicas" : 2,
  "nodes" : [
    { "host" : "127.0.0.1", "httpPort" : 8081, "weight" : 1.0 },
    { "host" : "127.0.0.1", "httpPort" : 8082, "weight" : 1.0 },
    { "host" : "127.0.0.1", "httpPort" : 8083, "weight" : 1.0 }
  ]
}
```

### 4.7 部署示例（docker-compose）

```bash
# 数据卷
docker volume create ww

# 三节点集群（镜像内 SPRING_PROFILES_ACTIVE=docker,nodeX）
docker compose up -d
```

三个节点共享卷 `ww`，通过 `application-nodeX.yml` 的 `storage.drives` 子目录物理隔离：

| 节点  | server.port | cache.port | storage.drives                      |
| ----- | ----------- | ---------- | ----------------------------------- |
| node1 | 8081        | 8701       | `/app/data/storage/node1/{d1,d2}` |
| node2 | 8082        | 8702       | `/app/data/storage/node2/{d1,d2}` |
| node3 | 8083        | 8703       | `/app/data/storage/node3/{d1,d2}` |

集群节点互访通过 Docker DNS 服务名（`ww-admin-node1/2/3`），见 `application-docker.yml`。

---

## 5. 使用方法

### 5.1 配置项（`application.yml` → `storage.*`）

| 配置项                              | 默认值             | 说明                                                            |
| ----------------------------------- | ------------------ | --------------------------------------------------------------- |
| `storage.drives`                  | 空                 | 本地 Drive（磁盘）根路径列表；空则用`root-dir` 单 Drive       |
| `storage.root-dir`                | `./data/storage` | drives 为空时的默认根目录                                       |
| `storage.drives-per-set`          | `0`              | 每 Set Drive 数；≤0 自动 = min(节点数, Drive 数, 4)            |
| `storage.crypto-enabled`          | `false`          | 加密开关（预留，当前透传）                                      |
| `storage.http-port-offset`        | `0`              | 节点 HTTP 端口偏移；0 时按`server.port - cache.port` 自动推导 |
| `storage.auth-token`              | 空                 | 节点间认证 token（`X-Storage-Token`），空不校验               |
| `storage.io-buffer-size`          | `65536`          | 直接内存缓冲区字节数（64KB~256KB 建议）                         |
| `storage.buffer-pool-max-size`    | `64`             | 缓冲池上限（总堆外 = size × 个数，默认 4MB）                   |
| `storage.metadata-cache-max-size` | `10000`          | 元数据 LRU 上限（约 2MB 堆内），0 禁用                          |

### 5.2 HTTP API（`/storage`，S3 风格）

| 方法       | 路径                                                           | 说明                               |
| ---------- | -------------------------------------------------------------- | ---------------------------------- |
| `PUT`    | `/storage/buckets/{bucket}`                                  | 创建 Bucket                        |
| `DELETE` | `/storage/buckets/{bucket}?recursive=true`                   | 删除 Bucket（非空需 recursive）    |
| `GET`    | `/storage/buckets`                                           | 列举 Bucket                        |
| `GET`    | `/storage/buckets/{bucket}`                                  | 获取 Bucket 元信息                 |
| `PUT`    | `/storage/buckets/{bucket}/objects/**`                       | 上传对象（流式，key 可含`/`）    |
| `GET`    | `/storage/buckets/{bucket}/objects/**`                       | 下载对象（本地 sendfile / 远端流） |
| `HEAD`   | `/storage/buckets/{bucket}/objects/**`                       | 对象元信息（仅响应头）             |
| `DELETE` | `/storage/buckets/{bucket}/objects/**`                       | 删除对象                           |
| `GET`    | `/storage/buckets/{bucket}/objects?prefix=&marker=&maxKeys=` | 列举对象（分页）                   |

响应约定：

- Bucket/对象管理类接口返回 `JsonResult`：`{type: success|warn|error, code, msg, data}`；
- 上传响应头带 `ETag`（MD5）、`x-obj-version`、`x-amz-meta-*`（自定义元数据原样透传）；
- 对象不存在：`GET/HEAD` 返回 404。

### 5.3 示例

```bash
# 创建 Bucket
curl -X PUT http://127.0.0.1:8081/storage/buckets/my-bucket

# 列举 Bucket
curl http://127.0.0.1:8081/storage/buckets

# 上传对象（自定义元数据）
curl -X PUT http://127.0.0.1:8081/storage/buckets/my-bucket/objects/images/logo.png \
     -H "Content-Type: image/png" \
     -H "x-amz-meta-owner: ww" \
     --data-binary @logo.png

# HEAD 对象
curl -I http://127.0.0.1:8081/storage/buckets/my-bucket/objects/images/logo.png

# 下载对象
curl http://127.0.0.1:8081/storage/buckets/my-bucket/objects/images/logo.png -o logo.png

# 列举对象（前缀过滤 + 分页）
curl "http://127.0.0.1:8081/storage/buckets/my-bucket/objects?prefix=images/&maxKeys=100"

# 删除对象
curl -X DELETE http://127.0.0.1:8081/storage/buckets/my-bucket/objects/images/logo.png

# 删除 Bucket（强制）
curl -X DELETE "http://127.0.0.1:8081/storage/buckets/my-bucket?recursive=true"
```

集群中**任意节点**都可执行上述请求，非主节点的请求会被透明转发到主节点。

### 5.4 集群扩缩容 API（`/storage/cluster`）

storage 模块提供集群动态扩缩容管理 API，挂在 `/storage/cluster` 下，由 `StorageClusterController` 暴露。

#### 操作流程

```
1. 挂维护牌（全集群拒客户端读写）
2. 执行扩缩容操作（scale-out / transfer / merge）
3. 查询迁移状态（可选）
4. 解除维护态
```

#### API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `PUT`  | `/storage/cluster/maintenance?enabled={true\|false}` | 设置维护态并广播到全集群 |
| `GET`  | `/storage/cluster/maintenance` | 查询当前维护态 |
| `GET`  | `/storage/cluster/topology` | 查询当前拓扑（节点列表、版本号、副本数） |
| `POST` | `/storage/cluster/scale-out?host=&httpPort=&weight=&rebalance=` | 添加存储节点（扩容） |
| `POST` | `/storage/cluster/transfer?sourceNodeKey=` | 转移节点数据（源节点保留在集群） |
| `POST` | `/storage/cluster/merge?sourceNodeKey=&purgeSource=` | 合并并下线节点（可选清理源端文件） |
| `GET`  | `/storage/cluster/migration/status` | 查询最近一次迁移结果 |
| `POST` | `/storage/cluster/failover?nodeKey=` | 手动强制 failover 指定节点（标记 DEAD + 路由重建 + 立即对账） |
| `POST` | `/storage/cluster/failover/cancel?nodeKey=` | 取消手动 failover（直接转 ALIVE，跳过反向同步） |
| `GET`  | `/storage/cluster/failover/status` | 查询 failover epoch + 各节点健康状态 |
| `GET`  | `/storage/cluster/health` | 节点健康状态快照（ALIVE/SUSPECT/DEAD/QUARANTINE + 失败次数） |

**参数说明**：

- `nodeKey` 格式：`{host}:{httpPort}`，例如 `127.0.0.1:8082`
- `scale-out.rebalance`：是否自动触发数据重平衡（默认 `true`）
- `merge.purgeSource`：是否清理源端本地文件（默认 `true`，迁移后删除源端对象文件）

#### 场景示例

**场景 1：增加节点 d**

```bash
# 1. 挂维护牌
curl -X PUT 'http://a:8081/storage/cluster/maintenance?enabled=true'

# 2. 添加 d 节点（自动 rebalance：约 1/(N+1) 的 key 主节点变为 d）
curl -X POST 'http://a:8081/storage/cluster/scale-out?host=10.0.0.4&httpPort=8081&weight=1.0'

# 3. 解除维护态
curl -X PUT 'http://a:8081/storage/cluster/maintenance?enabled=false'
```

**场景 2：转移 b 节点数据（b 保留在集群）**

```bash
curl -X PUT 'http://a:8081/storage/cluster/maintenance?enabled=true'
curl -X POST 'http://a:8081/storage/cluster/transfer?sourceNodeKey=10.0.0.2:8081'
curl -X PUT 'http://a:8081/storage/cluster/maintenance?enabled=false'
```

**场景 3：合并 b→c，下线 b**

```bash
curl -X PUT 'http://a:8081/storage/cluster/maintenance?enabled=true'
# 从拓扑移除 b → 重建 HRW → 迁移 b 上对象到新主 → 清理 b 上文件
curl -X POST 'http://a:8081/storage/cluster/merge?sourceNodeKey=10.0.0.2:8081&purgeSource=true'
curl -X PUT 'http://a:8081/storage/cluster/maintenance?enabled=false'
# 此时 b 节点已从拓扑移除，可物理下线 b 进程
```

**查询迁移状态**：

```bash
curl 'http://a:8081/storage/cluster/migration/status'
# 返回: { inProgress: false, lastResult: { mode, migrated, skipped, failed, bytes, durationMs, status, ... } }
```

**查询拓扑**：

```bash
curl 'http://a:8081/storage/cluster/topology'
# 返回: { version: 3, replicas: 2, nodes: [{nodeKey, host, httpPort, weight, self}, ...] }
```

#### failover 管理（手动触发与查询）

```bash
# 手动强制 failover b 节点（运维预判故障时隔离节点，与自动 failover 等效）
curl -X POST 'http://a:8081/storage/cluster/failover?nodeKey=10.0.0.2:8081'
# 返回: { nodeKey, failoverTriggered: true, epoch }

# 查询 failover 状态（epoch + 各节点健康状态）
curl 'http://a:8081/storage/cluster/failover/status'
# 返回: { epoch: 1, nodes: [{nodeKey, host, httpPort, self, status, failures}, ...] }

# 查询节点健康（与 failover/status 等效，语义更直观）
curl 'http://a:8081/storage/cluster/health'

# 取消手动 failover（确认节点正常或维护完成时，直接转 ALIVE，跳过反向同步）
curl -X POST 'http://a:8081/storage/cluster/failover/cancel?nodeKey=10.0.0.2:8081'
# 返回: { nodeKey, restored: true, epoch }
```

> 手动 failover 适用于运维预判故障或维护时隔离节点；自动 failover 由 `StorageFailoverManager` 周期心跳探测触发（见 3.12），无需人工介入。

#### 迁移报告字段

| 字段 | 说明 |
| --- | --- |
| `mode` | 迁移模式：`rebalanceAll` / `drainNode` / `drainAndPurge` |
| `migrated` | 成功迁移的对象数 |
| `skipped` | 跳过的对象数（已是新主 / 进度文件标记已完成） |
| `failed` | 失败的对象数 |
| `bytes` | 迁移的总字节数 |
| `durationMs` | 迁移耗时（毫秒） |
| `topologyVersion` | 迁移时的拓扑版本号 |
| `failures` | 失败详情列表（最多 20 条） |
| `status` | `success`（无失败）/ `partial`（部分失败）/ `error`（异常） |

#### 内部端点（节点间）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/internal/storage/topology?replicas=N` | 接收对端广播的拓扑，body 为 JSON 节点列表 |
| `POST` | `/internal/storage/maintenance?enabled=` | 接收对端广播的维护态切换 |
| `DELETE` | `/internal/storage/object/local?bucket=&key=` | 本地删除对象（不触发复制，供迁移清理源端） |
| `GET`  | `/internal/storage/ping` | 心跳探活（`StorageFailoverManager` 周期调用，2xx 视为存活） |
| `POST` | `/internal/storage/failover?nodeKey=&status=&epoch=` | 接收对端广播的 failover 事件，更新本地 nodeStatus + epoch + 重建路由 |
| `POST` | `/internal/storage/reverse-reconcile` | 触发原主反向同步（原主从新 primary 拉取落后对象补齐本地） |

这些端点走 `X-Storage-Token` 认证（与现有内部端点一致），不受维护态拦截。

#### 停服时间估算

迁移耗时 ≈ `待迁移数据量 / 内网带宽`。可在挂维护牌前用 `GET /storage/buckets/{bucket}/objects` 估算源节点数据量：

- 1TB 数据在万兆网约 100 秒；
- 1TB 数据在千兆网约 1000 秒。

建议在业务低峰期操作。若停服窗口过长，可后续优化为"离线预拷贝 + 增量迁移"，但一期暂不支持。

### 5.5 构建与测试

```bash
# 仅编译 storage 模块（依赖 common 已安装到本地仓库）
./mvnw -pl storage compile

# 全量测试
./mvnw test

# 打包镜像（Dockerfile 在 admin 模块）
```

---

## 6. 已知限制与设计取舍

- **复制为同步 best-effort**：主写成功即 ack，复制失败不阻断主写但入待重试队列（每节点上限 1 万条，满淘汰最旧），由后台线程按指数退避周期重放，副本冗余度可能暂时不足；
- **failover RPO > 0**：自动 failover（见 3.12）能在 ~15s 内将 primary 切到副本，但分区到 failover 期间原主未及复制的写会丢失（由 LWW via version 兜底丢弃旧版本）；原主恢复后经 QUARANTINE + 反向同步补齐，QUARANTINE 超时（5min）未完成则强制转 ALIVE，可能短暂读到落后数据；
- **Set 内单 Drive 落盘**：对象完整写入 Set 内一个 Drive（非纠删码分片），跨节点靠整对象复制冗余；`StorageSet` 已预留纠删码扩展点；
- **list 全量合并**：分布式 list 需扫描全部节点元数据并全量合并（无截断，避免大桶对象静默缺失），超大 bucket 场景下 list 延迟随对象数线性增长；
- **加密为预留**：当前 `IdentityCryptoCodec` 透传，`storage.crypto-enabled=true` 不产生实际加密；
- **扩缩容为停服模式**：迁移期间需挂维护态拒客户端读写，存在停服窗口（耗时 ≈ 数据量/带宽）；不支持在线 gossip + 增量 rebalance，换取大文件迁移的简单可靠；
- **迁移失败需手动重跑**：进度文件持久化支持崩溃续跑，但部分失败对象需运维判断失败原因（网络抖动 vs 磁盘满）后重跑同一 API；
- **拓扑广播为 best-effort**：广播失败仅告警，不阻断主流程；对端未收到广播时其本地拓扑仍为旧版本，后续该对端重启会从 `cluster-topology.json` 恢复最新拓扑；
- **副本冗余自动修复但有延迟**：周期对账 `reconcile()` 每 5 分钟主动扫描本地主对象、HEAD 比对副本 version、落后/缺失则补复制；failover 时会立即触发 `triggerReconcileNow()` 补副本，扩缩容后旧副本清理需等待下一个对账周期。
