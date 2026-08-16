# runtime-cluster 模块

> 包路径：`com.ww.runtime.cluster` ｜ 源码：`runtime/cluster/src/main/java/com/ww/runtime/cluster/`（8 个主类）
> 定位：**集群控制——节点能力启停与冻结（骨架）**：单节点生命周期 + 集群节点目录。

## 1. 模块职责

- 定义节点状态机（JOINING/ACTIVE/SUSPECTED/LEAVING/DEAD）的合法跃迁规则；
- 提供节点注册表抽象 `NodeRegistry` 及两种实现：进程内 `InMemoryNodeRegistry`（ConcurrentHashMap）与 PostgreSQL 的 `JdbcNodeRegistry`（plain SQL + 乐观锁）；
- 封装单节点运行时 `NodeRuntime`：注册 → 启动 gRPC 宿主 → 自检 → ACTIVE → 周期心跳 → 优雅退出；
- 采集本机 CPU/内存/磁盘指标（`ResourceMonitor`/`OsResourceMonitor`）；
- `ClusterSchema` 幂等初始化数据库表。

## 2. Maven 坐标与依赖

- 坐标：`com.ww:runtime-cluster:1.0-SNAPSHOT`
- 依赖：`runtime-common`、`runtime-proto`、slf4j-api；测试 junit/assertj
- 构建门禁：enforcer 禁 Spring/JPA/Caffeine。JDBC 实现仅用 JDK 自带 `javax.sql.DataSource`，不绑定驱动与连接池（由宿主注入）。

## 3. 关键类详解

### 3.1 NodeStateMachine（final 工具类，纯静态）

节点生命周期的**静态合法跃迁表**：

```
JOINING  → {ACTIVE, DEAD}
ACTIVE   → {SUSPECTED, LEAVING, DEAD}
SUSPECTED→ {ACTIVE, DEAD}
LEAVING  → {DEAD}
DEAD     → {}            （终态；同态跃迁一律非法）
```

| 方法 | 行为 |
|---|---|
| `static void check(NodeState from, NodeState to)` | 非法跃迁抛 `IllegalStateTransitionException`（消息含 `from -> to`）；null 参数抛 NPE |

### 3.2 NodeRegistry（接口，实现必须线程安全）

| 方法 | 语义 |
|---|---|
| `void register(NodeInfo)` | **upsert**：无论此前状态，一律重置为 JOINING 且 state_epoch=0（节点重启恢复语义） |
| `void heartbeat(NodeId, ResourceSnapshot)` | 刷新心跳时间与资源快照（心跳时刻取 `snapshot.updatedAtMillis()`）；未知节点抛 IAE |
| `void markState(NodeId, NodeState)` | 经状态机校验后跃迁且 `state_epoch + 1`；非法抛 `IllegalStateTransitionException` |
| `Optional<NodeInfo> get(NodeId)` | 查单节点 |
| `List<NodeInfo> listAlive()` | 过滤 DEAD 与 LEAVING 的存活视图 |
| `List<NodeInfo> listAll()` | 全量视图（含 DEAD 墓碑行） |
| `void tombstone(NodeId)` | 置 DEAD 并保留行，幂等 |

### 3.3 InMemoryNodeRegistry

- `ConcurrentHashMap<NodeId, NodeInfo> nodes`；所有"存在性检查 + 派生替换"在 `nodes.compute` 的原子语义内完成。
- `register` 强制重置 JOINING/epoch 0；`tombstone` 幂等（已 DEAD 不再前进 epoch）。

### 3.4 JdbcNodeRegistry（PostgreSQL 方言，plain SQL）

| 方法 | SQL 行为要点 |
|---|---|
| `register` | `INSERT ... ON CONFLICT (node_id) DO UPDATE`：强制 `state='JOINING'`、`state_epoch=0`；registered_at 取**数据库时钟** `EXTRACT(EPOCH FROM NOW())*1000` |
| `heartbeat` | `UPDATE rt_node SET cpu/mem/disk..., resources_at=?, heartbeat_at=? WHERE node_id=?`；命中 0 行抛 IAE |
| `markState` | **乐观锁 + 最多 8 次重试**：读当前 (state, epoch) → 状态机预校验 → `UPDATE ... SET state=?, state_epoch=state_epoch+1 WHERE node_id=? AND state=? AND state_epoch=?`（WHERE 即 CAS）→ 命中 0 行则重读重试 |
| `tombstone` | `UPDATE ... SET state='DEAD', state_epoch=state_epoch+1 WHERE node_id=? AND state<>'DEAD'`（幂等，不检查行数） |
| `mapRow` | NULL 兼容：cpu_percent null→0.0；resources_at null→回退 registered_at |

### 3.5 ResourceMonitor（接口）与 OsResourceMonitor

- 接口：`ResourceSnapshot snapshot()`。
- `OsResourceMonitor(File dataDir, Clock clock)`：基于 `com.sun.management.OperatingSystemMXBean` 与 `File.getTotalSpace/getUsableSpace`。
- 采集逻辑：
  - CPU：`getCpuLoad() * 100`，`clampPercent` 收敛 [0,100]（不可用的 -1 → 0.0）；
  - 内存：total−free，均 `max(0, ...)` 防负；
  - 磁盘：dataDir 所在卷 total−usable；
  - 采样时刻：`clock.nowMillis()`（可注入 TestClock）。

### 3.6 NodeRuntime（单节点运行时）

字段：self/host/configuredGrpcPort/weight/registry/grpcHost/monitor/clock/heartbeatIntervalMs；`AtomicBoolean started/shutdown`；`volatile ScheduledExecutorService`。

| 方法 | 行为 |
|---|---|
| `synchronized start()` | ① 初始快照 → ② `registry.register(JOINING, epoch 0)` → ③ 未运行则 `grpcHost.start(port)` → ④ 随机端口场景以实际端口**再次 register** → ⑤ 自检（isRunning && port>0，失败抛 `IllegalStateException("gRPC self-check failed")`）→ ⑥ `markState(ACTIVE)`（epoch 0→1）→ ⑦ 启动心跳线程 → ⑧ started=true |
| `int grpcPort()` | 宿主运行时返回绑定端口，否则返回配置端口 |
| `synchronized shutdown()` | 幂等：ACTIVE/SUSPECTED → `markState(LEAVING)`（异常仅 warn）→ 停心跳线程（shutdownNow + awaitTermination 5s）→ `grpcHost.stop()` → `tombstone(DEAD)` |
| `heartbeatOnce()`（私有） | `registry.heartbeat(self, monitor.snapshot())`；异常仅 warn 不中断循环 |

**线程模型**：单线程 daemon `ScheduledExecutorService`（线程名 `rt-heartbeat-{nodeId}`），`scheduleAtFixedRate` 固定速率。

正常生命周期状态序列：`JOINING(0) → ACTIVE(1) → LEAVING(2) → DEAD(3)`。

### 3.7 ClusterSchema

- `static bootstrap(DataSource)`：classpath 读 `sql/rt_cluster.sql`（UTF-8）→ 去 `--` 行注释 → 按 `;` 拆分 → 在 `pg_advisory_lock(4001, 1)` 会话级锁内逐条执行（finally unlock）。
- advisory lock 目的：跨节点并发 bootstrap 串行化，规避 PG `CREATE TABLE IF NOT EXISTS` 的 pg_type 唯一键竞态（3 节点场景实测暴露）。

## 4. 数据库表（`runtime/cluster/src/main/resources/sql/rt_cluster.sql`）

### rt_node

| 列 | 类型 | 约束/默认 | 说明 |
|---|---|---|---|
| node_id | VARCHAR(64) | PK | 节点 ID |
| host | VARCHAR(64) | NOT NULL | 主机名 |
| grpc_port | INT | NOT NULL | gRPC 端口 |
| state | VARCHAR(16) | NOT NULL | JOINING/ACTIVE/SUSPECTED/LEAVING/DEAD |
| state_epoch | BIGINT | NOT NULL DEFAULT 0 | markState/tombstone 各 +1 |
| weight | DOUBLE PRECISION | NOT NULL DEFAULT 1.0 | 调度权重 |
| cpu_percent / mem_total_bytes / mem_used_bytes / disk_total_bytes / disk_used_bytes | | nullable | 资源快照列 |
| resources_at / heartbeat_at | BIGINT | nullable | 资源/心跳时间戳 |
| registered_at | BIGINT | NOT NULL | 注册时间（DB 时钟） |

### rt_cluster_config

| 列 | 类型 | 约束 |
|---|---|---|
| cfg_key | VARCHAR(128) | PK |
| cfg_value | TEXT | nullable |
| version | BIGINT | NOT NULL DEFAULT 0 |

> `rt_cluster_config` 当前无读写实现，为预留表。

## 5. 与其他模块的关系

- 依赖 common（领域类型）与 proto（pom 声明；gRPC 宿主 `GrpcServerHost` 来自 proto 模块）。
- membership 通过三个函数式接口与之协作（无编译依赖）：`PeerDirectory` ← `listAlive()`、`StateSink` ← `markState()`、`selfInfo` ← `get(self)`。
- scheduler 依赖 `NodeRegistry`（listAlive/get 解析节点地址）；server 装配 `NodeRuntime`。

## 6. 测试覆盖

| 测试类 | 覆盖点 |
|---|---|
| `NodeStateMachineTest` | 8 种合法跃迁通过；25 组合中 17 种非法全抛异常；DEAD 终态；同态非法；null 拒绝 |
| `InMemoryNodeRegistryTest` | register upsert 重置、heartbeat 刷新保留状态、markState 跃迁+epoch、非法跃迁不污染 epoch、listAlive 过滤、tombstone 幂等、DEAD 后重新 register 可再次推进到 ACTIVE |
| `OsResourceMonitorTest` | TestClock 时间戳精确匹配；cpuPercent ∈ [0,100]；used ≤ total；默认 SystemClock 非空 |
| `NodeRuntimeTest` | 随机端口注册实际端口；复用已启动 host；重复 start 抛异常；心跳循环在 monitor 故障后存活；shutdown 停心跳、LEAVING→DEAD、幂等；未 start 直接 shutdown 无副作用 |
