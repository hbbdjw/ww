# runtime-proto 模块

> 包路径：`com.ww.runtime.proto`（生成代码）/ `com.ww.runtime.grpc`（宿主）
> 源码：`runtime/proto/src/main/proto/`（5 个 .proto）+ `runtime/proto/src/main/java/com/ww/runtime/grpc/GrpcServerHost.java`
> 定位：**运行时 typed gRPC 契约——runtime.v1 协议生成代码与服务器宿主**。

## 1. 模块职责

1. 通过 `protobuf-maven-plugin` 从 `src/main/proto/*.proto` 生成 `runtime.v1` 协议的 Java stub（消息类 + gRPC 服务基类），生成到包 `com.ww.runtime.proto`；
2. 提供轻量 gRPC 服务器宿主 `GrpcServerHost`：聚合多个 `BindableService` 并管理生命周期/端口。

## 2. Maven 坐标与构建

- 坐标：`com.ww:runtime-proto:1.0-SNAPSHOT`（父：`runtime-parent`）
- 依赖：`io.grpc:grpc-protobuf` / `grpc-stub` / `grpc-netty`（grpc-bom 1.62.2 统一管控）、`com.google.protobuf:protobuf-java:3.25.5`、`javax.annotation:javax.annotation-api:1.3.2`（Java 9+ 的 `@Generated`）、slf4j-api
- 构建：
  - extension：`kr.motd.maven:os-maven-plugin:1.7.1`（检测 `${os.detected.classifier}`，供 protoc 选择原生二进制）；
  - `org.xolstice.maven.plugins:protobuf-maven-plugin:0.6.1`：protoc 3.25.5 + protoc-gen-grpc-java 1.62.2，执行 `compile` 与 `compile-custom` 两个 goal；
  - maven-enforcer 同 common：禁 Spring/JPA/Caffeine。
- 所有 proto 统一：`syntax = "proto3"`、`package runtime.v1`、`option java_package = "com.ww.runtime.proto"`、`java_multiple_files = true`。

## 3. proto 定义清单（5 个文件、5 个 service、11 个 RPC）

### 3.1 health.proto

| 类型 | 名称 | 字段 |
|---|---|---|
| message | `HealthReq` | （空） |
| message | `HealthRes` | `bool healthy=1; string detail=2` |

| service | rpc | 类型 |
|---|---|---|
| `Health` | `Check(HealthReq) returns (HealthRes)` | unary |

### 3.2 cluster.proto

| message | 字段 |
|---|---|
| `ResourceSnapshotP` | `cpu_percent, mem_total_bytes, mem_used_bytes, disk_total_bytes, disk_used_bytes, updated_at_millis`（wire 表示，对应 common 的 `ResourceSnapshot`） |
| `NodeInfoP` | `node_id, host, grpc_port, state, state_epoch, weight, resources(ResourceSnapshotP)` |
| `Ack` | `ok, message` |
| `StartCapabilityReq` | `node_id, capability` |
| `StopCapabilityReq` | `node_id, capability` |
| `FreezeReq` | `node_id, capability, frozen` |

| service | rpc | 类型 |
|---|---|---|
| `ClusterControl` | `StartCapability` / `StopCapability` / `Freeze` | 均为 unary，返回 `Ack` |

> `NodeInfoP`/`ResourceSnapshotP` 被 membership.proto 复用（`import "cluster.proto"`）。

### 3.3 membership.proto

| message | 字段 |
|---|---|
| `HeartbeatReq` | `NodeInfoP self` |
| `HeartbeatAck` | `repeated NodeInfoP members`（对端的全量成员视图） |
| `WatchReq` | （空） |
| `MembershipEvent` | `string type`（JOINED/UPDATED/LEFT 等）`, NodeInfoP node` |

| service | rpc | 类型 |
|---|---|---|
| `Membership` | `Heartbeat(stream HeartbeatReq) returns (stream HeartbeatAck)` | **双向流** |
| `Membership` | `Watch(WatchReq) returns (stream MembershipEvent)` | 服务端流 |

### 3.4 migration.proto

| message | 字段 |
|---|---|
| `DataChunkP` | `task_id, seq, fin, data, checksum, checkpoint_offset` |
| `ChunkAckP` | `task_id, ack_seq, ok, message` |
| `FetchReq` | `capability, partition_id, from_checkpoint` |
| `TaskControlReq` | `task_id, action, payload_json` |
| `TaskAck` | `ok, message`（自定义定义，避免依赖 cluster.proto 的 Ack，解耦） |
| `ProgressReport` | `task_id, state, checkpoint_offset, items_done` |
| `ProgressSummary` | `ok, message` |

| service | rpc | 类型 |
|---|---|---|
| `Migration` | `TransferStream(stream DataChunkP) returns (stream ChunkAckP)` | **双向流**（目标端写入 + ack 重发） |
| `Migration` | `FetchPartition(FetchReq) returns (stream DataChunkP)` | 服务端流（源端拉取，支持断点） |
| `Migration` | `ControlTask(TaskControlReq) returns (TaskAck)` | unary（cancel） |
| `Migration` | `ReportProgress(stream ProgressReport) returns (ProgressSummary)` | 客户端流 |

### 3.5 placement.proto

| message | 字段 |
|---|---|
| `PlacementSnapshotReq` | `capability` |
| `PlacementP` | `capability, partition_id, node_id, role(PRIMARY/REPLICA), epoch` |

| service | rpc | 类型 |
|---|---|---|
| `PlacementWatch` | `WatchPlacements(PlacementSnapshotReq) returns (stream PlacementP)` | 服务端流 |

> `PlacementWatch` 的服务端实现为**下一阶段**工作（当前只有 proto 契约）。

### RPC 模式汇总

- Unary（5）：Health.Check、ClusterControl.×3、Migration.ControlTask
- 服务端流（3）：Membership.Watch、Migration.FetchPartition、PlacementWatch.WatchPlacements
- 客户端流（1）：Migration.ReportProgress
- 双向流（2）：Membership.Heartbeat、Migration.TransferStream

## 4. 关键类：GrpcServerHost

`com.ww.runtime.grpc.GrpcServerHost`（final，全部方法 `synchronized`）——轻量 gRPC 服务器宿主。

| 方法 | 行为 |
|---|---|
| `GrpcServerHost addService(BindableService)` | 注册服务；null 抛 IAE；已启动抛 `IllegalStateException("GrpcServerHost already started")`；链式返回 this |
| `void start(int port) throws IOException` | `ServerBuilder.forPort(port)` 聚合服务后 `build().start()`；**port=0 由 OS 随机分配**（测试友好） |
| `void stop()` | `server.shutdown()`；可再次 start（重启） |
| `void awaitTermination(long, TimeUnit)` | 委托 server |
| `int getPort()` | 未启动抛 `IllegalStateException`；返回实际绑定端口（随机端口场景必须在 start 后读取） |
| `boolean isRunning()` | 运行状态 |

**生命周期约束**：服务必须在 `start` 之前 `addService`；启动后再注册抛异常。这一约束决定了 server 模块启动顺序（见 [10-server.md](10-server.md) 第 5 节）。

## 5. 与其他模块的关系

被 5 个模块依赖：cluster、membership、scheduler、migration、server。
capability 与 placement 是纯逻辑/计算模块，**不直接依赖 proto**（不对外暴露 gRPC 端点）。

## 6. 测试覆盖

`GrpcServerHostTest`：
- `start(0)` 随机端口绑定 / `getPort()>0` / stop 后 isRunning=false 且 getPort 抛异常；
- 重复 start 抛 IllegalStateException 且原端口不受影响；
- 启动后 addService 抛异常；`addService(null)` 抛 IAE；
- stop 后可重启；
- `@AfterEach` 兜底清理（恢复中断标志）。

proto 生成的 stub 本身无单元测试，由 membership/migration/server 的集成测试覆盖。
