# runtime 模块 Code Wiki

> 本 Wiki 基于仓库 `f:\ww\ww` 中 `runtime/` 目录的实际源码逐文件分析生成，覆盖架构、模块职责、关键类与函数、依赖关系与运行方式。
> 对应顶层文档：[docs/architecture.md](../../../docs/architecture.md)（架构设计）、[distributed-runtime.md](../../../distributed-runtime.md)（蓝图）、[phase-record-foundation.md](../phase-record-foundation.md)（阶段记录）。

## 项目一句话简介

**distributed-runtime** 是一套"可编排、可迁移、可扩展、可恢复"的分布式应用基础运行时：Node 是资源容器、Capability 是可启停的能力、Partition 是逻辑数据单元、Placement 是所有权映射、Epoch 是所有权版本（Fencing Token）、Migration 是所有权转移、Scheduler 是 Desired→Actual 收敛器。支持单节点独立运行与多节点自动组集群。

## 技术栈速览

| 项 | 选型 | 版本 | 说明 |
|---|---|---|---|
| JDK | Java | 21（`maven.compiler.release`） | 核心模块纯 POJO + JDK |
| Spring Boot | 宿主 | 4.0.7 | 仅 runtime-server 使用（薄宿主） |
| gRPC | grpc-java | 1.62.2（grpc-bom 统一管控） | 节点间通信，四种 RPC 模式全覆盖 |
| Protobuf | protobuf-java | 3.25.5 | `runtime.v1` 协议 |
| Netty | netty-bom | 4.1.100.Final（runtime-parent 钉版） | 覆盖 Boot 4 默认 4.2.x，规避 HTTP/2 HPACK 流式问题 |
| PostgreSQL | 控制面元数据 | 16（测试容器）/ 生产任意 | plain JDBC，无 ORM |
| 构建 | Maven 多模块 | 3.9.0（wrapper） | mvnw / mvnw.cmd |

## 文档导航

| 文档 | 内容 |
|---|---|
| [01-architecture.md](01-architecture.md) | **架构总览**：总体架构图、核心概念、控制面/数据面、三大状态机、数据库表与 gRPC 服务总览 |
| [02-common.md](02-common.md) | **runtime-common**：值对象 / 枚举 / 异常体系 / 时钟抽象 |
| [03-proto.md](03-proto.md) | **runtime-proto**：5 个 .proto 契约、11 个 RPC、GrpcServerHost |
| [04-cluster.md](04-cluster.md) | **runtime-cluster**：节点状态机、NodeRegistry（内存/JDBC）、资源监控、NodeRuntime |
| [05-membership.md](05-membership.md) | **runtime-membership**：gRPC 心跳、gossip 视图合并、故障检测、事件发布 |
| [06-capability.md](06-capability.md) | **runtime-capability**：能力 8 态状态机、CapabilityManager、PlacementView fencing、MockCapability |
| [07-placement.md](07-placement.md) | **runtime-placement**：加权 HRW、Partitioner、PlacementStore CAS 切主、RebalancePlanner |
| [08-scheduler.md](08-scheduler.md) | **runtime-scheduler**：Leader 租约、ReconcileLoop 收敛、命令通道、DesiredStateWriter |
| [09-migration.md](09-migration.md) | **runtime-migration**：迁移 10 态状态机、MigrationEngine、gRPC 分片传输、断点续跑 |
| [10-server.md](10-server.md) | **runtime-server**：Spring 装配、启动时序、HTTP Admin API、配置全表 |
| [11-dependencies.md](11-dependencies.md) | **依赖关系**：模块依赖图、分层铁律、与 common/admin 的关系 |
| [12-getting-started.md](12-getting-started.md) | **构建与运行**：本地构建、单节点、多节点、验收脚本、测试 |

## 快速上手（TL;DR）

```powershell
# 1. 构建
.\mvnw.cmd -f runtime -DskipTests install

# 2. 单节点（进程内存储，无需 PG）
$env:RUNTIME_STORE='memory'; $env:RUNTIME_NODE_GRPC_PORT='0'; $env:SERVER_PORT='0'
java -jar runtime\server\target\runtime-server-1.0-SNAPSHOT.jar

# 3. 三节点黑盒验收（需 Docker 起临时 PG）
powershell -File scripts\runtime-scenario.ps1
```

详细内容见 [12-getting-started.md](12-getting-started.md)。

## 阅读建议

- **先读** 01-architecture.md 建立整体认知（核心抽象 + 三大状态机 + 一轮收敛时序）。
- **按数据流向读模块**：common → proto → cluster → membership → capability → placement → scheduler → migration → server。
- **运维/部署视角**直接看 10-server.md（配置与 API）与 12-getting-started.md（运行方式）。

## 术语表

| 术语 | 含义 |
|---|---|
| Node | 物理计算资源容器（≠角色），每节点含 gRPC 端口与资源快照 |
| Capability | 可运行能力（CACHE/STORAGE/API/...），当前实现 MOCK，可在任意 Node 启停 |
| Partition | 逻辑数据单元，默认 4096 个/能力命名空间，编号 `P000001..P004096` |
| Placement | 分区所有权映射（显式表 `rt_partition_placement`，非计算式路由） |
| Epoch | 所有权版本 / Fencing Token：切主 +1，旧 epoch 写入被拒绝 |
| Migration | 所有权转移：状态机驱动 + gRPC 分片传输 + 断点续跑 |
| Scheduler | Desired→Actual 收敛器：PG 租约竞选 Leader，无固定 Master |
| Desired / Actual | 期望态（管理面写入）/ 实际态（节点状态机产出），二者之差由 ReconcileLoop 收敛 |
| Reconcile | 调谐：对比 desired/actual → 生成命令与迁移计划 → 幂等执行 |
| Cutover | 切主：`PlacementStore.cutover` 以 epoch CAS 语义原子换主 |
| Fencing | 拒绝陈旧 Owner 写入：`StaleEpochException` |
| Leader Lease | `rt_scheduler_lease` 单行表乐观锁租约，全集群至多一个 Leader |
