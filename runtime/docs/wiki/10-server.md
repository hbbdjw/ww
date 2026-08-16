# 10. runtime-server — Spring 装配与节点进程

> 模块路径：`runtime/server`｜ artifactId：`runtime-server`
> 定位：**薄 Spring Boot 宿主（组装层）**。全部核心模块（cluster / membership / capability / placement / scheduler / migration）均为 Spring-free POJO，由本模块以构造注入方式组装为 Bean；它是 runtime 家族中**唯一允许依赖 Spring** 的模块。

## 1. 模块职责

- 提供节点进程入口（`RuntimeServerApplication`）与配置绑定（`RuntimeProperties`，前缀 `runtime.*`）。
- **装配**：`RuntimeAssembly`（唯一的宿主 `@Configuration`）把所有核心类构造注入组装为 Spring Bean，包括 jdbc/memory 双持久化模式切换。
- **编排**：`RuntimeBootstrapper`（`SmartLifecycle`，phase=`MAX_VALUE`）统一定义启动/停机顺序（gRPC 服务挂载 → 注册 → 能力 → 调度 → 迁移续跑）。
- **端口适配**：以 4 个小适配类把「模块 A 的接口」桥接到「模块 B 的实现」（见 §5）。
- **HTTP 管理面**：`RuntimeAdminController`（前缀 `/rt`）——只读状态 + 只写期望态。

包结构：

```
com.ww.runtime.server
 ├── RuntimeServerApplication    # main 入口
 ├── RuntimeProperties           # 配置 record（构造绑定 + 校验）
 ├── config/
 │   ├── RuntimeAssembly         # Bean 装配（含 jdbc/memory 双模式）
 │   └── RuntimeBootstrapper     # SmartLifecycle 启动/停机编排
 ├── grpc/
 │   ├── ClusterControlServer    # ClusterControl gRPC → CapabilityManager
 │   └── HealthServer            # Health gRPC（进程存活即健康）
 ├── migration/
 │   └── RuntimeMigrationCoordinator  # MigrationCoordinator → MigrationEngine
 ├── placement/
 │   └── PlacementViewAdapter    # PlacementView → PlacementStore
 └── web/
     └── RuntimeAdminController  # REST /rt 管理端点
```

## 2. RuntimeServerApplication（入口）

```java
@SpringBootApplication(scanBasePackages = "com.ww.runtime.server")
@ConfigurationPropertiesScan("com.ww.runtime.server")
public class RuntimeServerApplication {
    public static void main(String[] args) {
        SpringApplication.run(RuntimeServerApplication.class, args);
    }
}
```

- 扫描范围**仅限** `com.ww.runtime.server`——核心模块包（`com.ww.runtime.cluster` 等）不会被组件扫描，只能经 `RuntimeAssembly` 显式装配，保证核心模块零 Spring 依赖。
- 配置覆盖优先级：**环境变量（relaxed binding，`RUNTIME_NODE_ID` → `runtime.node.id`）> `application.yml` > `RuntimeProperties` 的 `@DefaultValue`**。

## 3. RuntimeProperties（配置全表）

record 构造绑定，紧凑构造器内建校验（非法值启动即失败）。每个子 record 各自带校验并配 `toXxxConfig()` 转换为核心模块配置（内建二次校验）。

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `runtime.node.id` | `node-1` | 节点唯一标识（非空） |
| `runtime.node.host` | `localhost` | 对外主机名/IP（非空） |
| `runtime.node.grpc-port` | `9100` | gRPC 监听端口；`0` = 随机端口（单测用） |
| `runtime.node.weight` | `1.0` | 节点权重（正有限数，HRW 加权用） |
| `runtime.store` | `jdbc` | `jdbc`（PostgreSQL）/ `memory`（进程内，单测演示） |
| `runtime.heartbeat-interval-ms` | `1000` | 心跳间隔 |
| `runtime.suspect-after-misses` | `3` | 连续错过 N 个周期判 SUSPECTED |
| `runtime.dead-after-misses` | `6` | 连续错过 N 个周期判 DEAD（须 ≥ suspect） |
| `runtime.scheduler.ttl-ms` | `10000` | Leader 租约 TTL |
| `runtime.scheduler.renew-interval-ms` | `3000` | 租约续约间隔 |
| `runtime.scheduler.reconcile-interval-ms` | `2000` | 调谐循环周期 |
| `runtime.partition-count` | `4096` | 分区总数 |
| `runtime.replication-factor` | `1` | 每分区副本数 |
| `runtime.min-move-ratio` | `0.10` | 触发扩容重平衡的最小移动比例 |
| `runtime.rebalance-cooldown-ms` | `30000` | 重平衡冷却时间 |
| `runtime.migration.max-concurrent` | `2` | 迁移引擎并发任务上限 |
| `runtime.migration.max-bytes-per-second` | `67108864` | 迁移限流（默认 64MB/s） |
| `runtime.migration.chunk-size` | `4194304` | 迁移分片大小（默认 4MB） |
| `runtime.migration.ack-window` | `8` | 分片传输确认窗口 |
| `runtime.capabilities.mock-desired` | `true` | 启动时自动把本节点 MOCK 期望态写为 ACTIVE |

## 4. RuntimeAssembly（Bean 装配）

唯一的宿主 `@Configuration`。**Bean 只负责「构造」**；带顺序的启动/停机编排归 `RuntimeBootstrapper`。

### 4.1 持久化双模式（`runtime.store`）

| 模式 | 激活条件 | 5 个仓库实现 | DDL |
|---|---|---|---|
| `jdbc`（默认） | `@ConditionalOnProperty(matchIfMissing=true)` | `JdbcNodeRegistry` / `JdbcCapabilityStateStore` / `JdbcPlacementStore` / `JdbcLeaseStore` / `JdbcMigrationTaskStore` | 启动时执行 5 个 `Schema.bootstrap`（CREATE TABLE IF NOT EXISTS，幂等） |
| `memory` | `runtime.store=memory` | 对应 `InMemory*` 实现 | 不建 DataSource、不执行 DDL、不触碰数据库（单测/单机演示） |

DataSource 由 `spring-boot-starter-jdbc` 自动装配（HikariCP），连接 `spring.datasource.*`。

### 4.2 关键装配决策

- **MembershipService 端口适配**（进程内适配，无需独立类）：
  - `PeerDirectory` = `registry.listAlive()` 排除 self；
  - `StateSink` = `registry.markState`；
  - `selfInfo` = 优先注册表当前视图（含最新 stateEpoch），未注册时用本地信息兜底。
- **命令通道**：`GrpcNodeCommandChannel` 内建 self 本地回退——`node==self` 走 `LocalNodeCommandChannel` 进程内直连，其余经 `NodeRegistry` 解析 `host:grpcPort` 远程下发。
- **MockCapability 的分区函数**：`key -> Partitioner.partition(key, props.partitionCount())`——与调度器同口径，保证 fencing 演示与迁移数据面一致。

### 4.3 Bean 依赖关系图（核心部分）

```
                    ┌── NodeId(self) ── Clock ── GrpcServerHost ── OsResourceMonitor
                    │
MembershipService ──┤（依赖 NodeRegistry + NodeRuntime + host）
MigrationServer ────┤（依赖 MockPartitionAdapter + MigrationTaskStore）
ClusterControlServer┤（依赖 CapabilityManager）
HealthServer ───────┤（依赖 ReconcileLoop）
                    │
NodeRuntime ────────┘（注册/端口绑定/心跳；start 由 Bootstrapper 驱动）

PlacementEngine / RebalancePlanner / PlacementStore
CapabilityManager（CapabilityStateStore） + MockCapability（PlacementViewAdapter）
GrpcNodeCommandChannel（LocalNodeCommandChannel + NodeRegistry）
MigrationEngine（MigrationTaskStore + MockPartitionAdapter + PlacementStore）
ReconcileLoop（lease + registry + capabilityStore + placementStore + engine/planner
              + commandChannel + migrationCoordinator + SchedulerConfig）
RuntimeBootstrapper（聚合以上全部，SmartLifecycle 编排启停）
```

## 5. 四个端口适配类

依赖倒置的落地：核心模块之间只依赖对方定义的**接口**（端口），由宿主提供**实现**（适配器）。

| 适配类 | 桥接 | 说明 |
|---|---|---|
| `PlacementViewAdapter` | capability 的 `PlacementView` → placement 的 `PlacementStore` | 行存在时透传属主 + epoch（fencing 语义）；行缺失返回 empty |
| `RuntimeMigrationCoordinator` | scheduler 的 `MigrationCoordinator` → migration 的 `MigrationEngine.migrateAndWait` | 失败透传 `false`（engine 内部置 FAILED），调度器下周期重算重试 |
| `ClusterControlServer` | gRPC `ClusterControl` → capability 的 `CapabilityManager` | 接收调度器下发的能力启停/冻结命令 |
| `HealthServer` | gRPC `Health` → `ReconcileLoop.isLeader` | 进程存活即 `healthy=true`，detail 带节点 id 与 leader 状态 |

## 6. RuntimeBootstrapper（启动/停机编排）

`SmartLifecycle`，`getPhase()=Integer.MAX_VALUE`：**Web 服务器就绪后最后启动、停机时最先执行**。启动序列任一步失败即抛异常终止容器启动（Fail-Fast）。

### 6.1 启动序列（7 步，顺序不可交换）

```
(1) bootstrapStore     自检：memory 模式跳过；jdbc 模式 PG SELECT 1 可达性检查
                       + 5 个 Schema bootstrap（幂等 DDL）
(2) host.addService    MigrationServer / ClusterControlServer / HealthServer
                       —— 必须在 host.start 之前（GrpcServerHost 启动后拒绝注册）
(3) membership.start   其内部才 addService 自身并启动心跳线程；
                       必须先于 nodeRuntime.start（后者会 host.start(port)）
(4) nodeRuntime.start  注册 JOINING → host 绑定端口（支持随机）→ ACTIVE → 心跳线程
(5) capability 注册    capabilityManager.register(mockCapability)
                       + 可选 mock-desired=true → DesiredStateWriter 写期望态 ACTIVE
(6) reconcileLoop      先手动 runOnce()（确定性首查：抢租约 + 首轮收敛，无并发）
                       → start()（守护调谐循环）
(7) migration.resumeAll()  断点续跑上次未完成的迁移任务
```

### 6.2 停机序列（倒序，全部幂等）

```
reconcileLoop.stop → membership.stop → grpcCommandChannel.close
→ nodeRuntime.shutdown（LEAVING → 停心跳 → host.stop → tombstone DEAD）
```

## 7. RuntimeAdminController（HTTP 管理面）

前缀 `/rt`。**§60 约束：管理面只写期望态（desired），不提供任何直接修改 actual 的端点**——actual 由节点能力状态机自治产出，偏差由调谐循环收敛。

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/rt/status` | 集群状态快照：调度器视图（leader/租约/能力记录/放置主表）+ 本节点注册视图 + 成员关系视图 + 活动迁移任务数 |
| GET | `/rt/nodes` | 全量节点视图（含 DEAD 墓碑行） |
| PUT | `/rt/nodes/{nodeId}/capabilities/{type}/desired` | 写期望态，body `{"desired":"ACTIVE"}` 或 `{"desired":"DISABLED"}`；非法输入与业务拒绝（清空最后 ACTIVE 节点保护）返回 400 + message；成功返回受影响的 `CapabilityRecord` |

示例：

```powershell
# 查看集群状态
curl http://127.0.0.1:8091/rt/status

# 关闭 node-1 的 MOCK 能力（触发 drain：freeze→迁移→cutover→停止）
curl -X PUT http://127.0.0.1:8091/rt/nodes/node-1/capabilities/MOCK/desired `
     -H "Content-Type: application/json" -d '{"desired":"DISABLED"}'
```

## 8. 测试

| 测试类 | 类型 | 说明 |
|---|---|---|
| `RuntimeServerIntegrationTest` | `@SpringBootTest`（`runtime.store=memory`，无需 PG） | 装配完整无异常（gRPC 绑定/心跳线程/leader 选举启动全部成功）；`/rt/status`、期望态写入往返、清空最后节点保护等 |
| `RuntimePostgresSmokeIT` | Testcontainers PG 16（Failsafe `*IT`） | 六表建表、节点注册、能力 desired/actual 往返、placement 重算 + 原子替换、租约互斥（5 用例） |

集成测试通过 `runtime.node.grpc-port=0`（随机端口）避免端口冲突，并缩短周期参数（心跳 200ms / TTL 5s / 调谐 300ms / 分区 64）加速收敛。

## 9. 设计要点

1. **核心 Spring-free**：`RuntimeAssembly` 是核心类接触 Spring 的唯一边界；未来脱离 Spring 独立运行只需重写这一个装配类 + Bootstrapper。
2. **顺序约束显式化**：gRPC「注册先于启动」「membership 先于 nodeRuntime」等顺序约束全部集中在 `RuntimeBootstrapper` 并以注释固化，Bean 之间无隐式启动顺序依赖。
3. **Fail-Fast 启动**：PG 不可达、DDL 失败、端口绑定失败等任一异常直接终止容器启动，避免带病运行。
4. **依赖倒置**：模块间耦合点全部收敛为 4 个小适配类，核心模块互不知晓对方实现。
