# runtime-scheduler 模块

> 包路径：`com.ww.runtime.scheduler` ｜ 源码：`runtime/scheduler/src/main/java/com/ww/runtime/scheduler/`（12 个主类）
> 定位：**调度器——放置决策与再平衡（骨架）**：控制面收敛器（Desired → Actual）。

## 1. 模块职责

- **Leader 选举**：基于 `rt_scheduler_lease` 单行表的乐观锁租约，全集群至多一个 ReconcileLoop 以 Leader 身份执行（无固定 Master）；
- **调谐收敛（Reconcile）**：每周期对比 desired/actual，生成 START/STOP/FREEZE 命令 + 迁移计划，幂等推进；
- **期望态写入校验**（`DesiredStateWriter`）：拦截管理面非法写入（如清空最后一个服务节点）；
- **命令下发**：本地直连 + gRPC 远程两种通道，统一返回布尔（失败不抛异常，下周期重试）。

**不直接搬数据**：分区搬迁 + 切主经 `MigrationCoordinator` 端口委托给 migration 模块（编译期不依赖 migration）。

## 2. Maven 坐标与依赖

- 坐标：`com.ww:runtime-scheduler:1.0-SNAPSHOT`
- 依赖：`runtime-common`、`runtime-proto`（ClusterControlGrpc）、`runtime-cluster`（NodeRegistry）、`runtime-capability`（CapabilityStateStore/CapabilityManager）、`runtime-placement`（PlacementStore/Engine/Planner）、slf4j-api。
- 构建门禁：enforcer 禁 Spring/JPA/Caffeine。**不依赖 runtime-migration**（MigrationCoordinator 端口反向解耦）。

## 3. 关键类详解

### 3.1 LeaseStore（接口）与两个实现

| 方法 | 语义 |
|---|---|
| `boolean acquireOrRenew(String owner, long ttlMillis)` | 租约无主或已过期（`expiresAt < now`）→ 抢占；自己是 owner → 续约；他人持有未过期 → false |
| `Optional<String> currentOwner()` | 有效租约 owner；无/过期返回 empty |

无显式 release——租约靠过期自然释放（Leader 宕机后最迟 TTL 内被接管）。

- **InMemoryLeaseStore**：单锁保护 owner/expiresAt；过期语义 `expiresAt < now`（恰等于视为仍持有，与 JDBC 版对齐）。
- **JdbcLeaseStore**：单行表 `rt_scheduler_lease`（id 恒 1）。抢占用条件 UPDATE 判命中行数：

```sql
UPDATE rt_scheduler_lease SET owner=?, expires_at_ms=?
WHERE id=1 AND (owner=? OR expires_at_ms < ?)
```

行不存在时先 `INSERT ... ON CONFLICT (id) DO NOTHING` 兜底（并发初始化仅一节点插入成功）再 UPDATE。
**注意：租约表无 epoch 字段**——Leader 唯一性仅靠 owner+expiresAt；epoch 是 Placement 表的字段（fencing 语义）。

### 3.2 ReconcileLoop（核心调谐循环）

**字段**：self、leaseStore、registry、capabilityStore、placementStore、engine（PlacementEngine）、planner（RebalancePlanner）、commands（NodeCommandChannel）、migration（MigrationCoordinator）、config、clock、`lastRebalanceAt`（每类型冷却门控）、worker/running/leader。

| 方法 | 行为 |
|---|---|
| `start()`（synchronized） | 幂等；创建守护线程 `runtime-scheduler-reconcile-{nodeId}` |
| `stop()` | running=false + 中断 + join(2s) + leader=false |
| `isLeader()` | 最近一周期的 Leader 身份 |
| `runOnce()` | **单周期入口（对宿主/测试开放，可外部驱动获得确定性）**：acquireOrRenew → 成功则逐类型 reconcile；失败静默跳过；异常吞掉记 warn |
| `snapshot()` | `ClusterStatus` record（leader/leaseOwner/capabilities/primariesByCapability），供管理端点 |
| `stopNode(node, type)` | freeze(false) → stopCapability → setActual(DISABLED) |
| `knownTypes()` | 从 capabilityStore.list() 提取去重排序的 CapabilityType |

**线程模型**：单守护线程；每周期后 `Thread.sleep(min(renewIntervalMs, reconcileIntervalMs))`——取小者既满足调谐节拍又保证租约不超时。

### 3.3 一轮 reconcile(type) 的完整算法

```
(a) 集合计算：
    alive       = registry.listAlive() 的节点 id 集
    desiredOn   = desired==ACTIVE 的节点集
    actualOn    = actual==ACTIVE 且存活的节点集
    desiredAlive= desiredOn ∩ alive

(b) 放置引导：primaries 为空且 desiredAlive 非空
    → engine.computePrimaries → placementStore.replaceAll（epoch=0）→ 记 lastRebalanceAt

(c) 待上线（desiredOn − actualOn，存活）：
    commands.startCapability(node, type) → 成功 setActual(ACTIVE)；失败 warn 下周期重试

(d) 待下线（actualOn − desiredOn）：
    先全部 freeze(true) 停新写
    ├─ desiredAlive 为空（无处可迁）→ 直接 stopNode 排空停止
    └─ 否则 computePrimaries(desiredAlive) → planner.plan 过滤 from∈toStop 的 moves
       → migration.migrateAndWait(moves)
       ├─ 成功：逐个 stopNode，更新 lastRebalanceAt
       └─ 失败：保留现状（仍冻结），warn 下周期重试
    本周期处理完下线即 return（扩容重平衡留待后续周期）

(e) 扩容重平衡（无待下线时）：
    actualOn 或当前环为空 → return
    plan(current, target) 为空 → return
    shouldRebalance(移动比例 ≥ minMoveRatio 且过 cooldown) 不满足 → return
    migration.migrateAndWait(moves) → 成功更新 lastRebalanceAt
```

### 3.4 DesiredStateWriter

`setDesired(node, type, desired)` 包装 `CapabilityStateStore.setDesired` 并施加集群级校验：

1. desired 只允许 **ACTIVE / DISABLED** 两个开关值（中间态由节点状态机自治产出）；违规抛 IAE；
2. **清空最后节点保护**：操作会导致"desired-ACTIVE 节点数为 0 且仍有 actual-ACTIVE 节点"时抛 `IllegalStateTransitionException`（无处可迁、服务中断）；全部节点已 DISABLE 后允许任意设置（重新启用）。

> 无独立 desired_state 表：desired/actual 统一存 `rt_capability_state`（capability 模块）。

### 3.5 NodeCommandChannel（接口）与两个实现

| 方法 | 语义 |
|---|---|
| `boolean startCapability(node, type)` | 远端语义：同步走完 DISABLED→STARTING→WARMING→ACTIVE |
| `boolean stopCapability(node, type)` | 内部负责 drain 链：ACTIVE→DRAINING→STOPPING→DISABLED |
| `boolean freeze(node, type, frozen)` | 维护开关（不参与状态机） |

约定：失败一律返回 false 不抛异常 → 调度器下周期重试（幂等收敛）。

- **LocalNodeCommandChannel**：仅 node==self 生效，直接委托进程内 CapabilityManager；stopCapability 内置 drain 链；已 DISABLED 直接 true（幂等）。
- **GrpcNodeCommandChannel**：node==self 回退 local（不绕网络）；否则从 registry 解析 host:grpcPort，`computeIfAbsent` 缓存 plaintext ManagedChannel（deadline 默认 5000ms），blocking stub 调 ClusterControl；`StatusRuntimeException` 记 warn 返回 false；`close()`（AutoCloseable）关闭全部通道。

### 3.6 MigrationCoordinator（端口）与 NoopMigrationCoordinator

`boolean migrateAndWait(String capability, List<PartitionMove> moves)`：
- **幂等**（同批 moves 重复执行安全）；**切主职责完全在实现内**（数据搬完调用 `PlacementStore.cutover` CAS）；
- 同步返回：全部完成 true；任一失败 false（调度器保留现状下周期重试）；空 moves 直接 true。

- **NoopMigrationCoordinator**：无数据能力的默认（直接 true，不切主）；真实宿主（server）用 `RuntimeMigrationCoordinator` 委托 `MigrationEngine`。

### 3.7 SchedulerConfig（record）

| 参数 | 默认 | 校验 | 含义 |
|---|---|---|---|
| ttlMillis | 10000 | >0 | 租约时长 |
| renewIntervalMs | 3000 | >0 且 < ttl | 续约间隔上限 |
| reconcileIntervalMs | 2000 | >0 | 调谐周期 |
| partitionCount | 4096 | >0 | 分区总数 |
| replicationFactor | 1 | >0 | 每分区副本数 |
| minMoveRatio | 0.10 | ∈ (0,1] | 触发扩容重平衡的最小移动比例 |
| rebalanceCooldownMs | 30000 | >=0 | 重平衡冷却 |

### 3.8 SchedulerSchema

`bootstrap(DataSource)` 执行 `sql/rt_scheduler.sql`，advisory lock `pg_advisory_lock(4001, 4)` 串行化。

## 4. 数据库表

### rt_scheduler_lease（单行表）

| 列 | 类型 | 约束 | 说明 |
|---|---|---|---|
| id | INT | PK DEFAULT 1, CHECK (id=1) | 恒 1，CHECK 阻止第二行 |
| owner | VARCHAR(64) | NOT NULL | 当前 Leader 节点 |
| expires_at_ms | BIGINT | NOT NULL DEFAULT 0 | 过期时刻（毫秒） |

## 5. 与其他模块的关系

- 依赖 common/proto/cluster/capability/placement；**不依赖 migration**（端口解耦，宿主注入 RuntimeMigrationCoordinator）；
- server 装配：LocalNodeCommandChannel + GrpcNodeCommandChannel、ReconcileLoop、DesiredStateWriter。

## 6. 测试覆盖

| 测试类 | 覆盖点 |
|---|---|
| `ReconcileLoopTest` | 引导放置+启动能力；排空节点只迁移该节点分区（moves 全 from=A，freeze true→false 子序列）；扩容上线新节点+冷却后重平衡（moves 全 to=D，未变分区不动）；非 Leader 无副作用；存储故障不中断调谐 |
| `ReconcileLoopLifecycleTest` | 守护线程 start/stop；**两 loop 选举唯一 Leader**；反复存储故障后仍收敛（真实时间 + awaitUntil） |
| `DesiredStateWriterTest` | desired 只接受 ACTIVE/DISABLED；有其他 desired-ACTIVE 节点时允许 DISABLE；排空最后服务节点被拒；全部 DISABLE 后允许任意设置 |
| `GrpcNodeCommandChannelTest` | 远端命令经 gRPC 下发（FakeClusterControl）；self 回退本地；未知节点 false |
| `LocalNodeCommandChannelTest` | start 委托状态机；他节点拒绝；stop 走 drain 链到 DISABLED；幂等；freeze 不改状态 |
| `InMemoryLeaseStoreTest` | 首次抢占/续约/他人被拒/边界过期（恰到点仍持有，+1 后接管）/参数校验/**200 轮并发竞争唯一 owner** |
| `SchedulerConfigTest` | 默认值/合法自定义/非正时间拒绝/renewInterval>=ttl 拒绝/拓扑参数拒绝 |
| `SchedulerSchemaTest` | DDL 资源打包且形状正确；bootstrap 拒绝 null DataSource |

测试辅助类：`RecordingCommands`（记录命令恒成功）、`CutoverMigrationCoordinator`（记录 moves + 按 CAS 逐分区 cutover）、`FlakyCapabilityStore`（前 N 次 list() 抛异常的装饰器）。
