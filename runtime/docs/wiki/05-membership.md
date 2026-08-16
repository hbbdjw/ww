# runtime-membership 模块

> 包路径：`com.ww.runtime.membership` ｜ 源码：`runtime/membership/src/main/java/com/ww/runtime/membership/`（6 个主类）
> 定位：**成员关系——心跳与成员变更事件（骨架）**：集群成员关系与故障检测。

## 1. 模块职责

- `MembershipService` 同时是 gRPC 服务端（`MembershipGrpc.MembershipImplBase`）与心跳客户端；
- 周期性双向流心跳与 peer 交换**全量视图**，采用 **gossip 悲观合并 + 直证例外**规则收敛成员视图；
- 基于静默时长判定 SUSPECTED/DEAD 并回写宿主注册表（`StateSink`）；
- 通过进程内监听器与 gRPC Watch 流发布成员变更事件。

## 2. Maven 坐标与依赖

- 坐标：`com.ww:runtime-membership:1.0-SNAPSHOT`
- 依赖：`runtime-common`、`runtime-proto`（HeartbeatReq/Ack、MembershipEvent、MembershipGrpc、NodeInfoP 等）、slf4j-api；间接获得 gRPC（ManagedChannel/StreamObserver 等）。
- 构建门禁：enforcer 禁 Spring/JPA/Caffeine。

## 3. 关键类详解

### 3.1 MembershipConfig（record）

| 字段 | 默认 | 校验 | 衍生 |
|---|---|---|---|
| `heartbeatIntervalMs` | 1000 | > 0 | — |
| `suspectAfterMisses` | 3 | >= 1 | `suspectTimeoutMs = suspect × interval`（默认 3000ms） |
| `deadAfterMisses` | 6 | >= suspect | `deadTimeoutMs = dead × interval`（默认 6000ms） |

### 3.2 MemberEvent（record）

- 字段：`String type`、`NodeInfo node`；类型常量 `NODE_ACTIVE` / `NODE_SUSPECTED` / `NODE_DEAD` / `NODE_UPDATED`。

### 3.3 PeerDirectory（@FunctionalInterface）

`List<NodeInfo> peers()` —— 返回除自身外应心跳的存活节点。宿主侧适配 `NodeRegistry.listAlive()`。

### 3.4 StateSink（@FunctionalInterface）

`void apply(NodeId id, NodeState newState)` —— 故障检测结果回写宿主注册表。宿主侧适配 `NodeRegistry.markState`。

### 3.5 MembershipService（核心，gRPC 服务端 + 心跳客户端一体）

**字段**：
- 注入：`self`、`selfInfo: Supplier<NodeInfo>`、`directory`、`stateSink`、`host: GrpcServerHost`、`config`、`clock`；
- 可变状态（均并发容器）：`view`（成员视图，含自身）、`lastHeard`（每 peer 最近收到消息时间）、`channels`（peer gRPC channel 缓存）、`listeners`（COW 列表）、`watchers`（gRPC Watch 流，COW 列表）；`viewLock` 串行化 merge/transitionLocal；
- 执行器：心跳线程 + 事件分发线程（均单线程 daemon）。

**生命周期**：

| 方法 | 行为 |
|---|---|
| `start()`（synchronized） | `host.addService(this)`（**必须先于 host.start**，因 GrpcServerHost 启动后拒绝注册）→ 创建心跳线程（`membership-heartbeat-{self}`）与事件分发线程（`membership-events-{self}`）→ `refreshSelf()` → `scheduleWithFixedDelay(heartbeatTick, 0, interval)`（**固定延迟**，非固定速率） |
| `stop()`（synchronized） | 停心跳执行器 → 关闭全部 PeerChannel → 清空 watchers 并逐个 `onCompleted()` → 优雅停事件分发（2s 超时 shutdownNow）→ 清空 view/lastHeard |
| `view()` | `Map.copyOf(view)` 不可变快照 |
| `addListener(Consumer<MemberEvent>)` | 注册进程内监听器 |

**gRPC 服务端**：

| RPC | 行为 |
|---|---|
| `heartbeat`（双向流服务端侧） | `onNext(HeartbeatReq)`：解析对端自报 `reported` → `lastHeard.put` + `merge(reported, directFromSource=true)` → 回 `HeartbeatAck`（本节点全量视图） |
| `watch`（服务端流） | 加入 watchers；`ServerCallStreamObserver.setOnCancelHandler` 在客户端取消时自动移除 |

**心跳客户端（heartbeatTick 每周期）**：

```
refreshSelf() → directory.peers()（异常降级空表）
→ 遍历 peers：sendHeartbeat(peer)
   · channelFor(peer)：host:port 未变复用、变了关闭重建（ManagedChannelBuilder.forAddress().usePlaintext()）
   · 双向流发 HeartbeatReq{self} 后 onCompleted()（单轮请求-应答半关闭）
   · onNext(ack)：lastHeard 更新 + 逐条 merge(member, 对端自身条目为直证)
   · onError：debug + resetConnectBackoff()（让 gRPC 立即重连）
→ 清理目录中已消失节点的 channel
→ detectFailures(now)
整体捕获 RuntimeException 仅 warn（保证调度循环不中断）
```

**故障检测（detectFailures）**：

- 仅监控 `lastHeard` 有记录的节点（纯 gossip 得知、无本地直接信号的节点不判定）；
- `silence = now − lastHeard`：
  - `silence > deadTimeoutMs` → `transitionLocal(DEAD)`
  - `silence > suspectTimeoutMs` 且当前非 SUSPECTED/DEAD → `transitionLocal(SUSPECTED)`
  - 否则当前为 SUSPECTED/DEAD → `transitionLocal(ACTIVE)`（恢复）
- `transitionLocal` **不提升 epoch**（本地判定等待节点自报恢复时由直证规则覆盖）。

**视图合并规则（gossip 悲观 + 直证例外）——`accept(local, incoming, directFromSource)`**：

| 场景 | 是否采纳 incoming |
|---|---|
| 直证（源节点自报，`directFromSource=true`） | `incoming.stateEpoch >= local.stateEpoch` 即采纳 |
| gossip：incoming epoch 更高 | 采纳 |
| gossip：incoming epoch 更低 | 拒绝 |
| gossip：同 epoch | 仅当 `pessimism(incoming) > pessimism(local)` 采纳（更悲观胜出） |

`pessimism`：ACTIVE=0，SUSPECTED/LEAVING/JOINING=1，DEAD=2。

其他细节：
- incoming.id == self 直接忽略（自身信息以本地为准）；
- merge 发现节点从非 ACTIVE 恢复为 ACTIVE 时，锁外回写 `stateSink.apply(id, ACTIVE)`；
- 事件经 `eventDispatcher` 单线程串行投递（监听器异常互不影响；watcher 取消/出错自动移除）。

**线程模型**：两个单线程 daemon 执行器（心跳 fixed-delay + 事件分发）；view/lastHeard/channels 并发容器 + viewLock 串行化读-改-写。

## 4. 心跳机制小结

| 参数 | 默认 | 说明 |
|---|---|---|
| 心跳周期 | 1000ms | `scheduleWithFixedDelay` |
| SUSPECTED 阈值 | 3 次未闻（3000ms） | 静默超过即疑似 |
| DEAD 阈值 | 6 次未闻（6000ms） | 静默超过即判死 |
| 恢复 | 重新收到消息 | SUSPECTED/DEAD → ACTIVE（transitionLocal，epoch 不变） |

## 5. 与其他模块的关系

- 依赖 common + proto；不依赖 cluster（通过 `PeerDirectory`/`StateSink`/`Supplier<NodeInfo>` 三个函数式接口在宿主层桥接）。
- server 装配时：PeerDirectory = `registry.listAlive()` 排除 self；StateSink = `registry.markState`；selfInfo 优先注册表当前视图、未注册用本地信息兜底。

## 6. 测试覆盖

| 测试类 | 覆盖点 |
|---|---|
| `MembershipConfigTest` | 合法/默认值；interval<=0、suspect<1、dead<suspect 拒绝；dead==suspect 允许 |
| `MembershipServiceTest` | 进程内双实例真实 gRPC 互连（短周期 100ms）：互心跳后双方视图互含 ACTIVE；静默 peer 依次 SUSPECTED→DEAD（事件与 StateSink 回写时序正确）；peer 重启（新端口）后恢复 ACTIVE（NODE_ACTIVE 事件出现在 NODE_DEAD 之后）；Watch 客户端收到事件；**合并规则单测**（直接驱动 merge）：缺失采纳/同 epoch 悲观胜/高 epoch 覆盖/直证同 epoch ACTIVE 覆盖本地 gossip SUSPECTED/恢复回写/自身条目不被他报覆盖 |
