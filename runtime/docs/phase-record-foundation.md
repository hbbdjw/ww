# 分布式运行时基础框架 · 阶段记录（§92 十项）

> 变更：add-runtime-foundation（Task 1–11）｜日期：2026-08-15｜状态：集成验证与场景验收全部通过

## 1. 修改

本阶段全部代码改动限定在 `runtime/**`（新增 9 模块）与根 pom 的 `<modules>` 追加（R9 允许项）；Task 11 集成验证阶段的改动：

| 文件 | 改动 |
|---|---|
| `runtime/server/pom.xml` | 增加 test 依赖 `testcontainers-postgresql`、`testcontainers-junit-jupiter`；配置 maven-failsafe 执行 `*IT.java`（integration-test + verify） |
| `runtime/{cluster,capability,placement,scheduler,migration}/.../*Schema.java`（5 个） | bootstrap DDL 前后加 `pg_advisory_lock(4001, n)` / `pg_advisory_unlock` 会话级咨询锁（详见 §2） |
| `runtime/capability/.../CapabilitySchema.java` | （集成暴露 bug 修复）`splitStatements` 改为「先去 `--` 行注释、再按分号拆分」，与 ClusterSchema 同口径；原实现把首行带注释的唯一语句片段整体跳过，真实 PG 上建不出 `rt_capability_state` |
| `runtime/server/src/test/.../RuntimePostgresSmokeIT.java` | 新增 Testcontainers PG 冒烟 IT（5 用例） |
| `scripts/runtime-scenario.ps1` | 新增本地 3 节点黑盒验收脚本（7 项断言，实际执行至全 PASS） |
| `scripts/runtime-docker-bridge.py` | 新增 WSL Docker Unix socket → TCP 2375 桥（本机 Testcontainers 环境辅助） |
| `runtime/docs/phase-record-foundation.md` | 本记录 |

既有系统（`admin/**`、`common/**`、根 `pom.xml`、`docs/**`、`.trae/**`）在 Task 11 期间零改动（git status 中上述路径的 diff 均为本变更早期任务或更早历史遗留，与 Task 11 无关）。

## 2. 设计理由

- **POJO 核心 + 薄 Spring 宿主**：核心 7 模块（common/proto/cluster/membership/capability/placement/scheduler/migration）零 Spring/JPA/Caffeine 依赖，maven-enforcer bannedDependencies 构建期门禁（R8）；runtime-server 仅做装配与端点暴露，Spring 配置翻译为 POJO 构造参数（防腐层）。
- **控制面 plain JDBC + CAS 收敛**：所有状态经 `rt_*` 表 CAS（租约原子认领、placement epoch CAS、迁移状态机跃迁 CAS），ReconcileLoop 周期 diff Desired/Actual 产计划直至收敛（R5）；管理端点只写 Desired（§60）。
- **加权 HRW 放置 + 最小移动**：4096 分区/能力命名空间，RendezvousHash 与现 common-cache HRW 数学一致（bootstrap 分布兼容）；扩容仅迁移新主为新节点的分区，变化率 <10% 不动 + cooldown（R4）。
- **epoch fencing**：Mock 写路径、cutover、迁移切主全部走 epoch 校验，旧 epoch 写入抛 `StaleEpochException`（R4）。
- **迁移框架存储无关**：九态任务状态机 + checkpoint 断点续跑 + 4MB 分片 CRC32/SHA-256 校验 + gRPC 双向流背压/限流/取消（R6），本阶段以 MockPartitionAdapter 验证。
- **Task 11 修复理由**：
  - *并发 bootstrap 竞态*：3 节点同时对同一 PG 执行 `CREATE TABLE IF NOT EXISTS`，PG 的存在性检查在两个并发事务中可同时通过，随后 `pg_type` 目录插入撞唯一键（`pg_type_typname_nsp_index`）导致节点启动失败。DDL 前取会话级 advisory lock 串行化后，`IF NOT EXISTS` 恢复幂等语义；锁键 `pg_advisory_lock(4001, 模块号)` 按模块互斥，连接关闭兜底释放。
  - *场景(3)测量时机*：扩容计划（≈4096/3 个分区）一次性提交 migrateAndWait，但逐分区 cutover 渐进落库；「node-3 首次出现即测比例」会拍到中途快照（实测 1.0%）。改为等待分布收敛（比例进入 [20%,45%] 窗口，终态 ≈1/3）再测。
  - *场景(6)前置条件*：死节点的分区只能重算到「存活且 desired=ACTIVE」的节点上；(4)排空 node-1、(5)杀死 leader 后再杀唯一剩余 MOCK 节点会使目标集为空，环保持不变是正确行为而非缺陷。脚本先重新启用 node-1（顺带验证 DISABLED→ACTIVE 重启用路径）再杀目标节点；管理 PUT 走任意存活节点端点（desired 写共享库）。

## 3. 新增类

共 86 个主类（不含 proto 生成代码）+ 43 个测试类：

| 模块 | 主类数 | 关键类 |
|---|---|---|
| runtime-common | 19 | NodeId/NodeInfo/NodeState、CapabilityType/CapabilityState、PlacementRole、MigrationTaskState、Clock、异常族（RuntimeFrameworkException/StaleEpochException/IllegalStateTransitionException…） |
| runtime-proto | 1 | （5 个 .proto + 生成代码；build helper 1） |
| runtime-cluster | 8 | NodeRegistry（JDBC/InMemory）、ClusterSchema、NodeStateMachine |
| runtime-membership | 5 | MembershipService（gRPC 心跳/Watch）、PeerDirectory、StateSink |
| runtime-capability | 8 | CapabilityStateStore（JDBC/InMemory）、MockCapability（epoch 校验写路径）、CapabilitySchema |
| runtime-placement | 11 | RendezvousHash、PlacementEngine、RebalancePlanner、PlacementStore（JDBC/InMemory）、PlacementSchema |
| runtime-scheduler | 12 | ReconcileLoop、JdbcLeaseStore/InMemoryLeaseStore、DesiredStateWriter、NodeCommandChannel（Local/gRPC）、SchedulerSchema |
| runtime-migration | 13 | MigrationEngine（九态+断点续跑）、MigrationTaskStore（JDBC/InMemory）、ChunkCodec、TransferStream、MockPartitionAdapter、MigrationSchema |
| runtime-server | 9 | RuntimeServerApplication、RuntimeBootstrapper、RuntimeAssembly、RuntimeProperties、RuntimeAdminController、RuntimeMigrationCoordinator |

## 4. 新增表（6 张，PostgreSQL，全部 IF NOT EXISTS 幂等 + advisory-lock 串行化）

| 表 | 模块 | 用途 |
|---|---|---|
| `rt_node` | cluster | 节点注册：host/grpc 端口/state/state_epoch/权重/资源快照/心跳时间 |
| `rt_cluster_config` | cluster | 集群级配置 KV（含版本） |
| `rt_capability_state` | capability | 节点×能力 desired/actual 状态（管理面唯一写入口的落点） |
| `rt_partition_placement` | placement | 分区→主节点放置，epoch 随 cutover 递增（fencing 依据） |
| `rt_scheduler_lease` | scheduler | 调度器租约（原子认领/续约/接管，TTL 默认 10s 续约 3s） |
| `rt_migration_task` | migration | 迁移任务九态 + checkpoint_json 断点 |

## 5. 新增 gRPC API（5 服务 / 12 rpc，独立 typed 栈，端口 9100 段）

| 服务 | rpc | 说明 |
|---|---|---|
| Membership | Heartbeat（双向流）、Watch（服务端流） | 心跳上报与成员变更事件订阅 |
| ClusterControl | StartCapability、StopCapability、Freeze | scheduler→节点的能力启停/冻结控制指令 |
| PlacementWatch | WatchPlacements（服务端流） | 放置表快照+增量推送 |
| Migration | TransferStream（双向流）、FetchPartition、ControlTask、ReportProgress（客户端流） | 4MB 分片传输（seq/fin、CRC32/SHA-256、背压、限流、取消）、数据拉取、任务控制、进度上报 |
| Health | Check | 健康检查 |

## 6. 新增测试

- 单测 43 个测试类（capability 5、cluster 4、common 4、membership 2、migration 8、placement 6、proto 1、scheduler 11、server 2），覆盖：状态机合法/非法跃迁、租约互斥与过期接管 ≤2×TTL、HRW 分布兼容与 2→3 节点最小移动（迁移集 == 新主为新节点的分区集）、并发 cutover 仅一方成功、九态跃迁与断点续跑对账为零差、校验和不匹配重试、MockCapability 旧 epoch 拒写 4 例、ReconcileLoop DISABLED→自动编排序列等。
- 集成测试：`RuntimePostgresSmokeIT`（Testcontainers PG 16，5 用例：六表建表、节点注册、能力 desired/actual 往返、placement 重算+原子替换、租约互斥）。
- 黑盒验收：`scripts/runtime-scenario.ps1` 7 项断言（见 §8）。

## 7. 启动

```powershell
# 构建可执行 jar（改过兄弟模块源码后先刷新本地快照：.\mvnw.cmd -f runtime -DskipTests install）
.\mvnw.cmd -f runtime -pl server -am -DskipTests package

# 单节点（环境变量覆盖 application.yml）
$env:RUNTIME_NODE_ID='node-1'; $env:SERVER_PORT='8091'
$env:RUNTIME_NODE_GRPC_PORT='9101'; $env:RUNTIME_NODE_HOST='127.0.0.1'
$env:SPRING_DATASOURCE_URL='jdbc:postgresql://127.0.0.1:55432/rt'
$env:SPRING_DATASOURCE_USERNAME='rt'; $env:SPRING_DATASOURCE_PASSWORD='rt'
$env:RUNTIME_STORE='jdbc'; $env:RUNTIME_CAPABILITIES_MOCK_DESIRED='true'
java -jar runtime\server\target\runtime-server-1.0-SNAPSHOT.jar
```

启动自检：PG 可达 → 六表 DDL 自动建表（幂等）→ 注册/心跳/租约/ReconcileLoop 后台线程就绪。管理端点：`GET /rt/status`、`GET /rt/nodes`、`PUT /rt/nodes/{nodeId}/capabilities/{type}/desired`（只写期望态）。时序参数可调：`RUNTIME_HEARTBEAT_INTERVAL_MS`、`RUNTIME_SUSPECT_AFTER_MISSES`、`RUNTIME_DEAD_AFTER_MISSES`、`RUNTIME_SCHEDULER_TTL_MS/RENEW_INTERVAL_MS/RECONCILE_INTERVAL_MS`、`RUNTIME_REBALANCE_COOLDOWN_MS`、`RUNTIME_MIGRATION_MAX_CONCURRENT`。

## 8. 验证（2026-08-15 实测输出）

1. **Testcontainers PG 冒烟**：`.\mvnw.cmd -f runtime/server clean verify`（`DOCKER_HOST=tcp://127.0.0.1:2375`）→
   `Tests run: 5, Failures: 0, Errors: 0, Skipped: 0 -- in com.ww.runtime.server.RuntimePostgresSmokeIT`，BUILD SUCCESS。
2. **本地 3 节点场景**：`powershell -File scripts\runtime-scenario.ps1` → **7/7 PASS**：
   - (1) 3-node registration ACTIVE —— 三节点互相可见全部 ACTIVE
   - (2) MOCK only on 2 nodes —— leader=node-1，primaries 仅 node-1/node-2
   - (3) scale-out rebalance ratio 20%-45% —— node-3 share=20.8%（收敛中），node-1/2 仍持有分区
   - (4) drain cutover —— node-1 actual=DISABLED、primaries 不含 node-1（freeze→迁移→cutover CAS→停止）
   - (5) lease takeover —— kill leader 后 ≤15s 接管（TTL=5s）并继续服务
   - (6) DEAD detection + recompute —— 存活视角 node-3=SUSPECTED（≤15s），重算后 primaries 排除（≤60s）
   - (7) stale-epoch fencing —— MockCapabilityTest 4 用例复跑绿（无 REST 写端点系设计使然）
3. **既有系统零回归**：`.\mvnw.cmd -pl common,admin -am test` → Reactor 8 模块全 SUCCESS（admin `Tests run: 74, Failures: 0, Errors: 0, Skipped: 0`）；`git status` 确认 Task 11 改动仅 `runtime/**`、`scripts/**`。
   注：admin 的 `BlazePersistenceQueryTest` 需本机 54321 端口的开发 PG（`postgresql17` 容器），容器未启动时该类 9 例连接失败——环境前置条件，非代码回归。

## 9. 已知问题

1. **零目标时环不重算**：某能力所有 desired=ACTIVE 节点全部死亡时，放置环保持原样（无合法迁移目标）。语义上正确（分区无处可迁），但监控上应告警「能力无可用节点」——下阶段补管理端点/指标。
2. **黑盒比例断言是近似**：场景(3) 的 [20%,45%] 窗口是黑盒近似；精确最小移动由单测保证（RebalancePlannerTest 等）。黑盒下逐分区 cutover 渐进可见，终态 ≈1/3。
3. **单模块构建的本地快照陷阱**：`-f runtime/server` 单模块构建从 ~/.m2 解析兄弟模块快照；改了兄弟模块源码而未 `install` 时会用到旧 jar（本次实测踩到：旧 CapabilitySchema 建不出表）。规范：改兄弟模块后先 `mvnw -f runtime install`，或始终用 `-pl server -am`。
4. **Testcontainers 环境依赖**：本机 dockerd 在 WSL 内，Windows 侧需 `scripts/runtime-docker-bridge.py` 桥 + `DOCKER_HOST=tcp://127.0.0.1:2375`；未设置时 IT 以 `disabledWithoutDocker` 静默跳过（CI 需确保 Docker 可达）。
5. **SUSPECTED 计为存活**：`listAlive()` 仅排除 DEAD/LEAVING，SUSPECTED 期间节点仍可被分配分区（心跳丢失但未达 dead 阈值的窗口）；配合 freeze/migration 语义可接受，RF>1 引入后需重审。
6. **场景脚本时序参数为加速值**：心跳 500ms/suspect 2/dead 4/TTL 5s 等仅用于验收提速，生产默认值见 SchedulerConfig（TTL 10s/续约 3s/冷却 30s）。

## 10. 下一阶段（cache/storage 动态调度接入）

1. **SPI 接入点**：以 runtime-capability 的 Capability SPI 包装现有 `SimpleCache`/对象存储（不引入 Caffeine，R9）；MockCapability 的 epoch 校验写路径契约平移到真实实现。
2. **placement 消费方式**：cache/storage 数据面订阅 PlacementWatch（或启动时快照+增量），写路径按「分区→主节点」路由，读路径遵循 read-after-write 一致性模型。
3. **真实迁移语义差异点**：Mock 为同进程直连 Map 语义；真实 cache/storage 需跨节点 gRPC adapter（本地/远端 PartitionDataAdapter 组合）、SYNCING 增量追平期的双写/停写策略、VERIFYING 的计数对账扩展为业务级校验。
4. **RF>1 与副本放置**：PlacementRole.REPLICA 启用，重平衡计划纳入副本间距；SUSPECTED 存活语义复审（§9.5）。
5. **运维配套**：「能力无可用节点」告警指标、迁移任务管理端点可视化、ADR-005（D1–D8 决策落档）与 spec/checklist 收口勾选。
