# distributed-runtime 架构设计

> 版本：0.1（Phase 0 输出）｜ 状态：已评审（依据《distributed-runtime蓝图可行性分析与改造难度评估》路径 C）
> 蓝图来源：`distributed-runtime.md`，本文件是其在 ww 仓库落地时的权威架构描述，含评审修正项 C1/C2/C5。

## 1. 总体架构

系统是一套**可编排、可迁移、可扩展、可恢复的分布式应用基础运行时**：

- **Node** = 物理计算资源容器（不等于角色）
- **Capability** = 可运行能力（CACHE / STORAGE / API / TASK...），可在任意 Node 上启停
- **Partition** = 逻辑数据单元（固定 4096 个/能力命名空间）
- **Placement** = 分区所有权映射（显式表，非计算式路由）
- **Epoch** = 所有权版本（Fencing Token，拒绝陈旧 Owner 写入）
- **Migration** = 所有权转移（Cache 逻辑迁移 / Storage 物理迁移）
- **Scheduler** = Desired → Actual 收敛器（PG 租约竞选，无固定 Master）

部署形态：单节点独立运行（全能力自持）；多节点自动组成集群（能力可分布在任意节点子集）。

```
                         PostgreSQL（控制面元数据）
                              │
                    ┌─────────▼─────────┐
                    │  Scheduler(租约)  │
                    │  Reconcile Loop   │
                    │  Placement Engine │
                    └─────────┬─────────┘
                              │ Migration Plan
          ┌───────────────────┼───────────────────┐
          ▼                   ▼                   ▼
   Migration Engine    Capability Manager      Router
          └───────────────────┼───────────────────┘
                              │ gRPC 集群网络（数据面）
       ┌──────────────┬───────┴──────┬──────────────┐
       ▼              ▼              ▼              ▼
    Node A          Node B        Node C         Node D
   [API/Storage]  [API/Cache]   [API/Cache]    [Storage]
```

## 2. 模块关系

按蓝图 §13 结构在 ww 仓库内新增 Maven 模块（路径 C：新骨架 + 器官移植）：

| 模块 | 职责 | 复用的现有资产 |
|---|---|---|
| runtime-common | 枚举/值对象/错误/工具（纯 POJO，零 Spring） | — |
| runtime-proto | 全部 .proto 与生成 stub | cluster-rpc 的构建模式 |
| runtime-cluster | 节点运行时/注册/生命周期 | FailoverManager（探活） |
| runtime-membership | 心跳/故障检测/成员 Watch | gossip 心跳逻辑 |
| runtime-placement | Partition/PlacementEngine/RebalancePlanner | `RendezvousHash`（加权 HRW） |
| runtime-scheduler | Scheduler 租约 + 收敛循环 | — |
| runtime-migration | MigrationEngine/状态机/传输 | `StorageMigrationService` |
| runtime-capability | Capability 生命周期管理 | — |
| runtime-cache | CacheCapability（分区化缓存） | `SimpleCache`（结构类型内核） |
| runtime-storage | StorageCapability（对象存储） | `ObjectStore`/`Drive`/`BucketStore` |
| runtime-routing | 分区路由 + API 加权路由 | — |
| runtime-api | 对外 API 网关语义 | — |
| runtime-observability | 指标/追踪/健康 | Micrometer |
| runtime-admin | HTTP 管理 API | admin Controllers + ww-ui |
| runtime-server | Spring Boot 宿主（装配一切） | `AdminApplication` 装配模式 |

依赖方向（铁律）：`server → {admin,api,capability 各模块} → {routing,migration,placement,membership} → {proto,common}`；任何模块不得反向依赖 server。

## 3. Control Plane / Data Plane

**严格分离。**

- **控制面**（PostgreSQL + Scheduler + gRPC 控制流）：Node/Membership/Capability/Placement/MigrationTask/Lease/Epoch/Config 元数据。数据库只是元数据事实源，**禁止**作为大数据搬运通道。
- **数据面**（gRPC Streaming + 本地磁盘 + 内存）：Cache/Object 业务数据、复制流、迁移流。大数据一律 `A → gRPC Stream → B`。
- 管理面 API 只写 **Desired State**，实际状态由 Scheduler 收敛产生，禁止直接 UPDATE 底层状态。

## 4. Node Lifecycle

```
JOINING → ACTIVE ⇄ SUSPECTED
   │         │
   │         ├→ DRAINING ⇄ ACTIVE(取消)
   │         │      │
   │         │      └→ LEAVING → DEAD
   │         └→ LEAVING / DEAD
   └→ DEAD
```

- JOINING：注册 → 健康检查 → 能力/资源发现 → 分配计算 → ACTIVE
- ACTIVE → DRAINING：运维发起（`DRAIN NODE`），系统自动停新分配 → 排空各能力 → 迁移数据 → 校验 → DRAINED
- SUSPECTED：心跳超时（悲观检测）；恢复转 ACTIVE，超限转 DEAD
- 所有状态跃迁落库；节点重启从 DB 恢复状态，不依赖 JVM 内存

## 5. Capability Lifecycle

```
DISABLED → STARTING → WARMING → ACTIVE → DRAINING → MIGRATING → STOPPING → DISABLED
                │         │         │         │                       │
                └─────────┴────→ FAILED ←───┴───────────────────────┘
```

- `cache-enabled=false` 之类只表达 **Desired State**；并发一致性（Stale Owner / Split Brain / Migration Race）一律由 Epoch + Lease + CAS + DB 事务解决（蓝图 §26）
- Cache 迁移为**逻辑所有权迁移**（Derived Data，miss 后回源重建），分级 warmup：NONE/ASYNC/SYNC/HOT_KEY_ONLY
- **修正 C1**：CacheCapability 的 store 分层——KV 可选 Caffeine；Hash/List/Set/ZSet/Stream 结构类型沿用 SimpleCache 内核；Stream 存量与消费组不支持逻辑迁移（保留旧主 + 文档限制）
- **修正 C5**：分区路由下读路径收紧为 owner-only（不再 fallback backup），保证 read-after-write 与 fencing 严格性

## 6. Partition Lifecycle

- 每能力独立命名空间：`CACHE:P000001..P004096`、`STORAGE:P000001..P004096`
- 路由：`key → xxHash64 → partitionId → placement 表 → owner(+replicas)`
- Partition 角色仅三种：PRIMARY / REPLICA / MIGRATING
- **修正 C2**：API 为无状态流量，不设分区——Router 维护节点加权路由表（A=70%/B=15%/C=15%/D=0%），节点故障自动重算

## 7. Placement

- 输入：Capability 节点集（ENABLED ∩ ALIVE）、weight、资源评分、分区数、RF、维护态
- 算法：Weighted Rendezvous Hashing（复用现有 `RendezvousHash`，输入从 key 换为 partitionId）
- 输出：`PartitionPlacement{partitionId, capability, nodeId, role, epoch, state}`
- 资源权重按能力定制（Cache: mem80/cpu10/net10；Storage: disk50/io25/net25；API: cpu50/mem20/net30）→ Capability Resource Score
- Rebalance 防抖：变化 <10% 不动、cooldown、最小移动阈值；优化目标是**少迁移优先于完美均匀**
- Bootstrap 策略：初始 placement 用现行 HRW 对 partitionId 计算，使新表与存量 key 分布近似一致，首切迁移量趋近于零

## 8. Migration

- Scheduler 决定 WHAT（P100 A→B），MigrationEngine 执行 HOW
- 状态机：`PENDING → PREPARING → SNAPSHOT → COPYING → SYNCING → CUTOVER → VERIFYING → COMPLETED`（+FAILED/CANCELLED），跃迁合法性强校验
- Storage（权威数据）：快照 sequence=N → 全量复制 ≤N → 增量日志追平 → Freeze → 校验和（SHA-256，大对象 chunk 级）通过 → epoch++ 换主 → 观察期 → 清理源端。**先验证后删除，禁止为速度先删源**
- 传输：gRPC 双向流分片（4MB/片），支持 Resume/Retry/Checksum/Backpressure/Progress/Cancel
- 限流：`migration.max-concurrent`、`migration.max-bandwidth` 防止拖垮业务
- 幂等：taskId + partitionId + epoch + operationId；管理 API 带 RequestId 防重复建任务

## 9. Failure Recovery

| 场景 | 策略 |
|---|---|
| 节点宕机 | 心跳超时 → 标记 DEAD → Placement 重算 → Cache Rebuild / Storage Replica Recovery |
| Scheduler 宕机 | 租约过期 → 其余节点竞选接管 → 重新 Reconcile |
| 迁移中断 | 节点重启后 load task → 按 checkpoint 续跑 |
| Split Brain | 旧 Owner 持旧 epoch 写入 → 写路径统一经 OwnershipManager 校验 → 拒绝 |
| 控制面 DB 不可用 | 数据面以最后已知 placement 降级运行 + 告警；调度面拒绝新任务 |
| planned migration vs failure recovery | 严格分离：前者走 Freeze/Cutover，后者走重算/重建 |

## 10. 一致性分级

- **强一致**：Ownership、Epoch、Lease、Migration State、Placement Cutover（DB 事务 + 乐观锁）
- **最终一致**：Metrics、Cache Warmup、资源统计、成员视图传播

## 11. 阶段门控

Phase 0-16 顺序执行，不跳阶段；每阶段按"设计→编码→测试→运行→验证→提交"闭环并输出十项验收记录。第一阶段范围：Phase 0（本文档）+ Phase 1（骨架）+ Phase 2（Node 运行时）。
