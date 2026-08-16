# Runtime 模块深度代码审查与架构分析报告

> 审查对象：`runtime`（分布式集群节点弹性伸缩、动态调度与能力生命周期管理核心框架）
> 审查日期：2026-08-15
> 审查范围：runtime-common / proto / cluster / membership / capability / placement / scheduler / migration / server 全部子模块主代码
> 审查方法：静态走查 + 确定性复现测试（6 个复现测试全部实际运行验证，见文末汇总表）

## 0. 结论摘要

| # | 缺陷 | 严重度 | 模块 | 复现结果 |
|---|------|--------|------|----------|
| 1 | `MembershipService.dispatch` 在 COW 迭代器上 `remove()`，坏 watcher 导致后续 watcher 永久静默丢事件 | 高 | membership | ❌ 测试失败（0/2 事件送达） |
| 2 | 能力命令不幂等：崩溃遗留 `STARTING` 态无任何命令可达，永久僵死；重试 start 恒失败 | 高 | capability / server | ❌ 两用例均失败 |
| 3 | DEAD 节点分区治愈被 `minMoveRatio` 门控拦截，N≥11 节点时故障分区永不迁走 | 高 | scheduler | ❌ 测试失败（17/256 分区滞留 DEAD 节点） |
| 4 | 租约只在周期开始续约，长迁移期间租约过期产生双 leader 脑裂窗口 | 严重 | scheduler | ✅ 证明型测试通过（双 leader + 双侧副作用证据链成立） |
| 5 | `CapabilityManager.transition` 非原子 check-then-act，并发命令导致模块钩子执行两次 | 高 | capability | ❌ 测试失败（start 钩子执行 2 次） |
| 6 | `NodeRuntime.start()` 失败残留 JOINING 孤儿，`shutdown()` 拒绝清理 | 中高 | cluster | ❌ 测试失败（shutdown 后仍 JOINING） |

另有 4 项附加风险观察（未单独编写复现测试），见第 7 节。

所有复现测试均为确定性测试（无随机 sleep 赌竞态，唯一例外是缺陷 4 采用"证明型"设计），可直接纳入 CI 作为回归防线（修复后应转绿）。

---

## 1. 缺陷一：watcher 事件分发在 CopyOnWriteArrayList 迭代器上 remove()，分发链被永久打断

### 1.1 问题概述

`MembershipService.dispatch` 的事件分发任务在 `CopyOnWriteArrayList` 的快照迭代器上调用 `it.remove()`。COW 迭代器**不支持** remove，必抛 `UnsupportedOperationException`。一旦任一 watcher 进入"被取消"或"onNext 抛异常"状态，分发任务立即中断逃逸：

- 坏 watcher **永远无法被移除**（两条移除路径都抛异常）；
- 列表中**其后注册**的所有 watcher 从此收不到任何事件——静默丢失，无日志告警（`LOG.debug` 级别且异常在 catch 内二次抛出，日志都打不出来）。

对集群的直接影响：依赖 watch 流的对等节点/控制面将永久看到陈旧的成员视图。

### 1.2 代码定位

[runtime/membership/src/main/java/com/ww/runtime/membership/MembershipService.java](../membership/src/main/java/com/ww/runtime/membership/MembershipService.java) 第 471–483 行（`dispatch` 提交给 `eventDispatcher` 的 Runnable 内）：

```java
for (Iterator<StreamObserver<MembershipEvent>> it = watchers.iterator(); it.hasNext(); ) {
    StreamObserver<MembershipEvent> watcher = it.next();
    try {
        if (watcher instanceof ServerCallStreamObserver<?> sco && sco.isCancelled()) {
            it.remove();          // ← 路径A：COW 迭代器 remove() 抛 UnsupportedOperationException
            continue;             //          → 落入 catch → catch 内再次 it.remove() 二次抛出并逃逸
        }
        watcher.onNext(proto);
    } catch (RuntimeException e) {
        it.remove();              // ← 路径B：onNext 抛异常后同样抛 UnsupportedOperationException 逃逸
        LOG.debug("watcher removed after error: {}", e.toString());  // 永远执行不到
    }
}
```

`watchers` 声明为 `private final List<StreamObserver<MembershipEvent>> watchers = new CopyOnWriteArrayList<>();`（第 79 行）。

真实世界的触发条件并不苛刻：
- 客户端取消 watch 流与 dispatch 快照迭代并发（`watch()` 里 `setOnCancelHandler` 的移除与分发任务的快照存在竞态窗口）；
- `isCancelled()` 检查与 `onNext()` 之间传输层关闭（check-then-act 天然竞态），`onNext` 抛 `StatusRuntimeException`。

### 1.3 复现方案

测试文件：[runtime/membership/src/test/java/com/ww/runtime/membership/WatcherDispatchIteratorBugReproTest.java](../membership/src/test/java/com/ww/runtime/membership/WatcherDispatchIteratorBugReproTest.java)

真实触发路径是竞态，测试以确定性方式驱动同一代码路径：

1. 启动单节点 `MembershipService` + `GrpcServerHost`（随机端口）；
2. 通过反射向 `watchers` 列表注入一个 `onNext` 抛 `StatusRuntimeException(CANCELLED)` 的坏观察者（模拟流已死）；
3. 再经真实 gRPC 通道注册 watcher2（保证迭代顺序为 `[坏, watcher2]`），轮询列表确认注册完成；
4. 注册监听器作为锚点（监听器先于 watchers 执行、不受 watcher 循环异常影响）；
5. 连续 `merge` 两个事件 Y、Z，断言 watcher2 收到 Y 和 Z。

运行命令：

```powershell
cd f:\ww\ww
.\mvnw.cmd -pl runtime/membership test "-Dtest=WatcherDispatchIteratorBugReproTest" "-Dsurefire.failIfNoSpecifiedTests=false"
```

实际运行输出（缺陷成立的证据）：

```
WatcherDispatchIteratorBugReproTest.brokenWatcherBreaksEventDispatchForSubsequentWatchers:167
[坏 watcher 不应阻断 watcher2 的事件分发；……watcher2 实际收到 0/2 个事件]
Expecting value to be true but was false
```

锚点断言 `listenerEvents == 2` 先行通过，证明两次 dispatch 任务确实执行了，仅在 watcher 循环处中断——排除了"事件根本没产生"的解释。

### 1.4 根因分析

1. **API 误用**：`CopyOnWriteArrayList` 的迭代器是创建时的数组快照，`remove()/set()/add()` 全部抛 `UnsupportedOperationException`——这是 JDK 明确的契约。代码按 `ArrayList` 的习惯写了 `it.remove()`。
2. **异常路径自我放大**：路径 A 中 `it.remove()` 抛出后落入同一个 catch，catch 里又调 `it.remove()`，二次异常直接逃逸 Runnable，被线程池吞掉——连 debug 日志都没机会输出，故障完全静默。
3. **无自愈机制**：坏 watcher 留在列表里，每次分发都在同一位置中断，其后所有 watcher 的丢失是**永久性**的，直到服务重启。
4. 次要问题：`isCancelled()` 与 `onNext()` 之间的 check-then-act 竞态意味着即使修复了 remove，也必须容忍 `onNext` 随时抛异常——错误处理必须按 watcher 隔离。

### 1.5 修复建议

改为"逐个隔离 + 值删除"（`CopyOnWriteArrayList.remove(Object)` 是支持的）：

```java
List<StreamObserver<MembershipEvent>> dead = new ArrayList<>();
for (StreamObserver<MembershipEvent> watcher : watchers) {
    try {
        if (watcher instanceof ServerCallStreamObserver<?> sco && sco.isCancelled()) {
            dead.add(watcher);
            continue;
        }
        watcher.onNext(proto);
    } catch (RuntimeException e) {
        dead.add(watcher);
        LOG.debug("watcher removed after error: {}", e.toString());
    }
}
if (!dead.isEmpty()) {
    watchers.removeAll(dead);   // 按值删除，COW 语义安全
}
```

要点：
- 单个 watcher 出错只影响它自己，循环不中断；
- `removeAll` 按值删除是 COW 支持的操作，且与并发注册不冲突（新注册的 watcher 不在 dead 中）；
- `stop()` 中已有的 `watchers.clear()` + 逐个 `onCompleted()` 路径保持不变。

修复后本测试应转绿，建议保留为回归测试。

---

## 2. 缺陷二：能力命令不幂等，崩溃遗留 STARTING 态成为不可达死角

### 2.1 问题概述

调度器通过 `NodeCommandChannel` 向节点下发 start/stop 命令驱动能力状态收敛，但命令实现**不幂等、不完备**：

- `start` 只接受 `DISABLED` 起点。节点已经 `ACTIVE` 时（例如上一次 start 实际成功、但 gRPC 5 秒 deadline 超时导致 leader 认为失败并重试）重试恒失败——`actual` 与 `desired` 永远无法收敛为 ACTIVE；
- 节点崩溃/重启遗留在 `STARTING`、`WARMING`、`MIGRATING` 等中间态时，`start`（要求 DISABLED）与 `stop`（要求 STOPPING/DRAINING/FAILED）**双双拒绝**——没有任何命令能把该能力拉回收敛轨道，形成永久状态僵死。

### 2.2 代码定位

[runtime/capability/src/main/java/com/ww/runtime/capability/CapabilityManager.java](../capability/src/main/java/com/ww/runtime/capability/CapabilityManager.java)：

```java
public void start(CapabilityType type) {                    // L110
    CapabilityModule module = module(type);
    transition(type, CapabilityState.STARTING);             // requireFrom: 只允许 DISABLED→STARTING
    ...
}
```

`transition`（L224）内 `requireFrom` 校验静态跃迁表；`stop`（L169）只接受 `FAILED`（直接回 DISABLED）与 `DRAINING/STOPPING` 链。

服务端入口 [runtime/server/src/main/java/com/ww/runtime/server/grpc/ClusterControlServer.java](../server/src/main/java/com/ww/runtime/server/grpc/ClusterControlServer.java)：

```java
public void startCapability(StartCapabilityReq request, StreamObserver<Ack> responseObserver) {  // L35
    respond(responseObserver, () -> manager.start(...));    // 已 ACTIVE 时抛 IllegalStateTransitionException → Ack=false
}

public void stopCapability(StopCapabilityReq request, StreamObserver<Ack> responseObserver) {    // L40
    ...
    CapabilityState state = manager.state(type);
    if (state != CapabilityState.DISABLED) {
        if (state == CapabilityState.ACTIVE) {
            manager.drain(type);
        }
        manager.stop(type);                                 // state=STARTING/WARMING/MIGRATING 时抛异常 → Ack=false
    }
}
```

`LocalNodeCommandChannel` 与 gRPC 通道同构，问题一致。调度侧 `ReconcileLoop` 收到 `false` 只记 warn 日志、下周期原样重试——永远失败。

### 2.3 复现方案

测试文件：[runtime/scheduler/src/test/java/com/ww/runtime/scheduler/CommandIdempotencyDeadlockBugReproTest.java](../scheduler/src/test/java/com/ww/runtime/scheduler/CommandIdempotencyDeadlockBugReproTest.java)

两个用例（经 `LocalNodeCommandChannel` → `CapabilityManager` 真实链路）：

1. **`startCommandShouldBeIdempotentWhenAlreadyActive`**：start 成功至 ACTIVE 后再次下发 start（模拟 deadline 超时重试），断言第二次返回 `true`（幂等语义）；
2. **`stuckStartingStateIsUnreachableByAnyCommand`**：直接 `store.setActual(NODE, MOCK, CapabilityState.STARTING)` 模拟崩溃遗留，断言 `start` 与 `stop` **至少一个**返回 `true`（收敛可达性）。

运行命令：

```powershell
.\mvnw.cmd -pl runtime/scheduler test "-Dtest=CommandIdempotencyDeadlockBugReproTest" "-Dsurefire.failIfNoSpecifiedTests=false"
```

实际输出：两用例均失败——第二次 start 返回 false；STARTING 态下 start/stop 全部返回 false。

### 2.4 根因分析

1. **命令语义错配**：调度器视角的命令是"使节点达到目标态"（declarative converge），实现却是"执行一次状态机跃迁"（imperative transition）。declarative 调谐循环要求命令幂等且对任意起点可收敛，imperative 实现两条都不满足。
2. **gRPC deadline 放大问题**：`GrpcNodeCommandChannel` 使用 5 秒 blocking deadline，`module.start()/warm()` 耗时超过 5 秒时（真实能力预热很常见）leader 看到超时→判失败→重试→恒失败，而节点侧能力其实已经 ACTIVE。
3. **状态机完备性缺口**：静态跃迁表没有为中间态（STARTING/WARMING/MIGRATING）定义任何"逃生边"，进程崩溃恢复后这些状态成为死角。

### 2.5 修复建议

1. **start 幂等化**（`ClusterControlServer.startCapability` 与 `LocalNodeCommandChannel` 同步改）：

```java
CapabilityState state = manager.state(type);
switch (state) {
    case ACTIVE, WARMING -> { /* 已在目标轨道，幂等成功 */ }
    case DISABLED, FAILED -> manager.start(type);
    case STARTING -> manager.awaitStart(type, timeout);  // 等待在途启动收敛后按结果应答
    default -> throw new IllegalStateException("unexpected state " + state + " for start");
}
```

2. **stop 全态可达**：非 DISABLED 的任何状态都允许强制收敛——中间态直接 `markFailed → DISABLED` 或新增 `FORCE_STOP` 跃迁边，保证"停"这个操作永远可达；
3. 命令响应携带执行后的真实状态（`Ack` 增加 `actualState` 字段），leader 以响应状态而非 true/false 判定收敛，天然免疫 deadline 超时误判；
4. 长期方案：将 `NodeCommandChannel` 的命令语义整体改为 `converge(node, type, targetState)`，由节点侧负责从任意当前态收敛到目标态。

---

## 3. 缺陷三：DEAD 节点分区治愈被扩容门控拦截，故障分区永不迁走

### 3.1 问题概述

节点被判定 DEAD 后，其承载的分区应当尽快迁移到存活节点（故障治愈）。但 `ReconcileLoop.reconcile` 把**故障治愈**与**扩容重平衡**混在同一条路径（步骤 f），共用 `shouldRebalance` 门控（最小移动比例 `minMoveRatio` + 冷却时间）。单节点故障的移动占比约为 `1/N`：当节点数 N≥11 时 `1/N < 0.10`（默认 `minMoveRatio`），门控**永远拒绝**——DEAD 节点上的分区永远不会被迁走，读写持续打到死节点，直到人工干预或再挂一个节点凑够比例。

### 3.2 代码定位

[runtime/scheduler/src/main/java/com/ww/runtime/scheduler/ReconcileLoop.java](../scheduler/src/main/java/com/ww/runtime/scheduler/ReconcileLoop.java) 第 292–316 行（步骤 f）：

```java
// (f) 扩容重平衡：无待下线且当前环与 actualOn 目标环有偏差
...
long lastRun = lastRebalanceAt.getOrDefault(type, 0L);
if (!planner.shouldRebalance(moves.size(), config.partitionCount(), config.minMoveRatio(),
        lastRun, config.rebalanceCooldownMs(), clock.nowMillis())) {
    return;                                   // ← DEAD 节点的治愈 moves 也在这里被丢弃
}
if (migration.migrateAndWait(capability, moves)) { ... }
```

节点 DEAD 后：`desiredOn`（存活且期望 ACTIVE）与当前环产生偏差，moves = 落在 DEAD 节点上的分区 ≈ `partitionCount / N`。以测试参数 `256 分区 / 16 节点`计约 16–17 个分区，占比 6.6% < 10% → `shouldRebalance` 返回 false → 直接 return，且**不推进任何状态**，下周期原样重演。

`minMoveRatio` 的设计意图是抑制扩容引起的小批量抖动迁移（成本收益不划算），把它套用在故障治愈上是语义错配——治愈的代价模型完全不同（不迁 = 数据不可用）。

### 3.3 复现方案

测试文件：[runtime/scheduler/src/test/java/com/ww/runtime/scheduler/DeadNodeHealingGateBugReproTest.java](../scheduler/src/test/java/com/ww/runtime/scheduler/DeadNodeHealingGateBugReproTest.java)

步骤（全部使用 `InMemory*` 仓库 + `TestClock`，确定性）：

1. 构造 16 节点集群、256 分区、`minMoveRatio=0.10`、默认冷却；
2. 所有节点 desired/actual = ACTIVE，`runOnce()` 引导初始放置环；
3. `registry.tombstone(deadNode)` 模拟故障摘除；
4. 循环 5 次：`clock.advance(cooldown + 1s)`（确保冷却已过，排除冷却因素）+ `runOnce()`；
5. 断言：所有分区的 primary 均不含 DEAD 节点。

运行命令：

```powershell
.\mvnw.cmd -pl runtime/scheduler test "-Dtest=DeadNodeHealingGateBugReproTest" "-Dsurefire.failIfNoSpecifiedTests=false"
```

实际输出（复现成功）：

```
[DEAD 节点的分区应在治愈周期内全部迁走] 
实际 17/256 分区仍指向 DEAD 节点
```

冷却已排除、租约已持有、迁移器可用，唯一拦截点就是 `shouldRebalance` 的比例门控。

### 3.4 根因分析

1. **单一门控承载两种语义**：`shouldRebalance(moves, ...)` 只看移动数量，不区分移动的**原因**。扩容偏差（锦上添花）与故障治愈（雪中送炭）被同等对待。
2. **比例阈值与集群规模耦合**：单点故障的移动比例 = `1/N`，随集群规模单调递减。该缺陷在集群越大时越必然触发——与"大集群更需要自愈"的直觉完全相反。
3. 步骤 (e)（显式下线）不受此门控影响，但 DEAD 节点不走下线流程（没人下发 stop，节点已经死了），只能落到步骤 (f)。

### 3.5 修复建议

将治愈与重平衡分流：

```java
// 步骤 (f) 前拆分 moves
List<PartitionMove> healing = moves.stream()
        .filter(m -> m.from() != null && !alive.contains(m.from()))
        .toList();
List<PartitionMove> rebalance = moves.stream()
        .filter(m -> m.from() == null || alive.contains(m.from()))
        .toList();

// 治愈：不受 minMoveRatio / cooldown 门控，立即执行
if (!healing.isEmpty()) {
    if (migration.migrateAndWait(capability, healing)) {
        LOG.info("healing migration for {} done ({} partitions off dead nodes)", capability, healing.size());
    } else {
        LOG.warn("healing migration for {} failed; retry next cycle", capability);
    }
    return;
}
// 扩容重平衡：保留原门控
if (!rebalance.isEmpty() && planner.shouldRebalance(...)) { ... }
```

补充建议：治愈迁移应优先选择轻量路径（副本提升优先于全量拷贝），并在 `RebalancePlanner` 层对 `from==DEAD` 的 move 单独记账，便于监控"治愈延迟"指标。

---

## 4. 缺陷四：租约只在周期开始续约，长迁移期间租约过期导致双 leader 脑裂

### 4.1 问题概述

`ReconcileLoop.runOnce` 仅在周期**开始**获取/续约租约，随后同步执行 `migration.migrateAndWait(...)`。迁移是跨节点拷贝数据的长操作，耗时轻易超过租约 TTL（默认 10s）。期间：

- 租约过期，另一个节点竞选成功成为新 leader 并开始调谐；
- 旧 leader 的 `migrateAndWait` 返回后**不复核租约**，继续执行 `stopNode` 等副作用（解冻、停能力、回写 actual）；
- 两个 leader 同时对集群下发命令、改写放置表与能力状态——脑裂。且系统无 fencing token，存储层无法拒绝过期 leader 的写入。

### 4.2 代码定位

[runtime/scheduler/src/main/java/com/ww/runtime/scheduler/ReconcileLoop.java](../scheduler/src/main/java/com/ww/runtime/scheduler/ReconcileLoop.java)：

```java
public boolean runOnce() {                                    // L147
    boolean acquired = false;
    try {
        acquired = leaseStore.acquireOrRenew(self.value(), config.ttlMillis());  // ← 周期开始一次性续约
        leader = acquired;
        if (acquired) {
            for (CapabilityType type : knownTypes()) {
                reconcile(type);            // ← 内部 migrateAndWait 可阻塞任意长时间，期间无续约
            }
        }
        ...
    }
}
```

下线路径（第 277–287 行）：

```java
if (migration.migrateAndWait(capability, moves)) {   // 阻塞可超 TTL
    for (NodeId node : toStop) {
        stopNode(node, type);                        // ← 返回后不复核租约，直接下发副作用
    }
    ...
}
```

`JdbcLeaseStore.acquireOrRenew` 按 TTL 判断归属：旧 leader 超过 TTL 未续约即被抢占——存储层行为正确，问题完全在调用方的租约使用方式。

### 4.3 复现方案

测试文件：[runtime/scheduler/src/test/java/com/ww/runtime/scheduler/LeaseSplitBrainBugReproTest.java](../scheduler/src/test/java/com/ww/runtime/scheduler/LeaseSplitBrainBugReproTest.java)

设计为"证明型"测试（断言双 leader 窗口与双侧副作用**存在**，故缺陷成立时测试通过）：

1. 两个 `ReconcileLoop` 实例 S1/S2 共享同一 `InMemoryLeaseStore`、`TestClock`、注册表与放置仓库；
2. 注入慢迁移 lambda 给 S1：在 `migrateAndWait` 内部 `clock.advance(TTL + 1s)` 模拟迁移耗时超过租约，然后**主动调用 `loop2.runOnce()`**——此时 S1 租约已过期，S2 竞选成功；
3. S1 迁移返回后继续执行 `stopNode`；
4. 断言证据链：
   - `loop1.isLeader() && loop2.isLeader()` 同时为真（双 leader 窗口）；
   - `commands1` 与 `commands2` 都实际下发了命令（双侧副作用）；
   - S1 在失去租约后仍执行了 `stopped` 命令（过期 leader 写副作用）。

运行命令：

```powershell
.\mvnw.cmd -pl runtime/scheduler test "-Dtest=LeaseSplitBrainBugReproTest" "-Dsurefire.failIfNoSpecifiedTests=false"
```

实际输出：BUILD SUCCESS（证据链全部成立）。注：修复该缺陷后此测试语义需反转（断言不出现双 leader），建议改造为回归测试。

### 4.4 根因分析

1. **租约使用模式错误**：把租约当"周期门票"而非"持续授权"。任何可能超过 TTL 的同步操作（迁移、慢 IO、GC 停顿）都会造成授权真空，而代码在操作返回后不复核。
2. **无 fencing**：`LeaseStore` 没有暴露单调递增的 fencing token（epoch），节点命令通道与存储写入都不携带租约纪元，存储层无法拒绝过期 leader 的写入——即使调度层复核了租约，竞态窗口内的写入仍会落地。
3. **单线程循环放大阻塞**：调谐循环是单线程串行，迁移阻塞期间连日志心跳都没有，更不可能续约。

### 4.5 修复建议

三层防御，建议全部落实：

1. **后台续约线程**：`runOnce` 持有租约期间启动守护续约任务（间隔 `TTL/3`），迁移等长操作不再阻塞续约；
2. **操作后复核**：`migrateAndWait` 返回后、执行任何副作用前复核租约归属，失租立即中止并回滚冻结状态：

```java
if (migration.migrateAndWait(capability, moves)) {
    if (!leaseStore.isOwnedBy(self.value())) {
        LOG.warn("lease lost during migration; aborting side effects");
        return;
    }
    for (NodeId node : toStop) { stopNode(node, type); }
}
```

3. **fencing token**：`LeaseStore` 每次授予/续约返回递增 epoch；`NodeCommandChannel` 命令与放置/能力状态写入携带 epoch，存储层 CAS 校验（`UPDATE ... WHERE lease_epoch <= ?`），从存储层根绝过期写入。

---

## 5. 缺陷五：CapabilityManager 状态跃迁非原子，并发命令导致模块钩子重复执行

### 5.1 问题概述

`CapabilityManager.transition` 是"读状态 → 校验合法性 → 写新状态"三步非原子序列，且 `start/stop/drain` 等方法无同步。gRPC 服务端使用线程池处理并发命令，两条相同的 start 命令并发到达时，双方都读到 `DISABLED`、都通过校验、都执行 `module.start()`——能力模块的启动钩子（建连接、开端口、加载数据）被执行两次，后果从重复资源分配到端口冲突不等。

### 5.2 代码定位

[runtime/capability/src/main/java/com/ww/runtime/capability/CapabilityManager.java](../capability/src/main/java/com/ww/runtime/capability/CapabilityManager.java) 第 224–231 行：

```java
private void transition(CapabilityType type, CapabilityState to) {
    CapabilityState from = state(type);          // 1) 读
    if (!isLegalTransition(from, to)) {          // 2) 校验
        throw new IllegalStateTransitionException(from, to, ...);
    }
    store.setActual(self, type, to);             // 3) 写 —— check-then-act 无任何同步
}
```

`start(type)`（L110）在两次 `transition` 之间执行 `module.start()` / `module.warm()` 真实钩子。`InMemoryCapabilityStateStore`/JDBC 实现的 `setActual` 均为无条件覆盖写，不提供 CAS。leader 重试（缺陷二场景）、双 leader（缺陷四场景）、人工运维命令都会制造并发输入。

### 5.3 复现方案

测试文件：[runtime/capability/src/test/java/com/ww/runtime/capability/CapabilityManagerTransitionRaceReproTest.java](../capability/src/test/java/com/ww/runtime/capability/CapabilityManagerTransitionRaceReproTest.java)

确定性竞态复现（不靠 sleep 赌窗口）：

1. `GatedStore` 包装真实状态仓库：`armed` 标志 + `CyclicBarrier(2)`，武装后**前两次** `get()` 在屏障处会合——保证两个线程同时读到 `DISABLED`、同时通过校验；
2. 两个线程并发调用 `manager.start(MOCK)`；
3. `MockCapability` 记录 `starts` 计数；
4. 断言 `module.starts == 1`（启动钩子只应执行一次）。

注：门闩在 `manager.register()` 完成后才武装，避免注册路径的 `store.get` 消耗屏障配额。

运行命令：

```powershell
.\mvnw.cmd -pl runtime/capability test "-Dtest=CapabilityManagerTransitionRaceReproTest" "-Dsurefire.failIfNoSpecifiedTests=false"
```

实际输出（复现成功）：

```
[模块 start 钩子只应执行一次] 
expected: 1 but was: 2
```

### 5.4 根因分析

1. **经典 check-then-act 竞态**：状态校验与状态写入分离且无互斥，两个并发调用可以各自独立地"校验通过"。
2. **状态机与副作用耦合在锁外**：`module.start()` 钩子在两次 `transition` 之间执行，即便 transition 原子化，钩子区间仍需整体互斥，否则两条命令仍可在 STARTING 阶段重叠执行模块初始化。
3. **仓库层无 CAS 兜底**：`setActual` 无条件覆盖，丢失了"以存储层做最终仲裁"的机会。

### 5.5 修复建议

按 CapabilityType 粒度加锁，把"校验 + 钩子 + 落库"整段纳入临界区：

```java
private final ConcurrentMap<CapabilityType, Object> typeLocks = new ConcurrentHashMap<>();

public void start(CapabilityType type) {
    synchronized (typeLocks.computeIfAbsent(type, t -> new Object())) {
        CapabilityModule module = module(type);
        transition(type, CapabilityState.STARTING);
        try {
            module.start();
            transition(type, CapabilityState.WARMING);
            module.warm();
            transition(type, CapabilityState.ACTIVE);
        } catch (Exception e) {
            markFailed(type);
            ...
        }
    }
}
```

配套加固：
- `CapabilityStateStore` 增加 `compareAndSetActual(node, type, expect, update)`，`transition` 改用 CAS，失败即视为竞态落败（配合缺陷二的幂等语义返回"已在目标轨道"）；
- 幂等入口（缺陷二修复）与 per-type 锁是互补关系：锁防并发钩子重复执行，幂等防串行重试报错。

---

## 6. 缺陷六：NodeRuntime 启动失败残留 JOINING 孤儿，shutdown 拒绝清理

### 6.1 问题概述

`NodeRuntime.start()` 的顺序是**先注册 JOINING，再启动 gRPC 宿主**。若端口绑定失败（端口占用、权限等）抛 `IOException`：

1. 注册表中残留一条 JOINING 记录：心跳永远不会到来（心跳线程没启动）、无人推进它的状态；`listAlive()` 视其为存活节点——放置环可能把它算进去，调度器会向一个不存在的地址下发命令并等待超时；
2. `started` 未置位，`shutdown()` 的第一行守卫直接返回，**不做 tombstone**——孤儿记录无法通过正常停机路径清理，只能等外部故障检测（若故障检测只看心跳，JOINING 态的处置取决于实现）或人工改库。

滚动部署中端口冲突、配置错误并不罕见，该缺陷会持续制造"幽灵节点"。

### 6.2 代码定位

[runtime/cluster/src/main/java/com/ww/runtime/cluster/NodeRuntime.java](../cluster/src/main/java/com/ww/runtime/cluster/NodeRuntime.java)：

```java
public synchronized void start() throws IOException {                 // L82
    if (started.get()) { throw new IllegalStateException(...); }
    ResourceSnapshot initialSnapshot = monitor.snapshot();
    registry.register(NodeInfo.of(self, host, configuredGrpcPort,
            NodeState.JOINING, 0L, weight, initialSnapshot));         // ← 先注册
    if (!grpcHost.isRunning()) {
        grpcHost.start(configuredGrpcPort);                           // ← 后绑定；失败抛 IOException，JOINING 残留
    }
    ...
}

public synchronized void shutdown() {                                 // L126
    if (!started.get() || !shutdown.compareAndSet(false, true)) {
        return;                                                       // ← 启动失败场景从这里直接返回，无任何清理
    }
    ...
}
```

### 6.3 复现方案

测试文件：[runtime/cluster/src/test/java/com/ww/runtime/cluster/NodeRuntimeStartFailureOrphanReproTest.java](../cluster/src/test/java/com/ww/runtime/cluster/NodeRuntimeStartFailureOrphanReproTest.java)

1. 启动一个 `GrpcServerHost` 占用随机端口，取得端口号；
2. 用**同一端口**构造 `NodeRuntime` 并 `start()`——断言抛 `IOException`（绑定失败）；
3. 断言孤儿存在：`registry.get(SELF)` 状态为 JOINING，且 `listAlive()` 包含它；
4. 调用 `runtime.shutdown()`，断言状态应变为 DEAD（期望行为）。

运行命令：

```powershell
.\mvnw.cmd -pl runtime/cluster test "-Dtest=NodeRuntimeStartFailureOrphanReproTest" "-Dsurefire.failIfNoSpecifiedTests=false"
```

实际输出（复现成功）：

```
[start 失败后 shutdown 未做任何清理：应 tombstone 为 DEAD，实际仍为 JOINING]
expected: DEAD but was: JOINING
```

### 6.4 根因分析

1. **先写后建的顺序缺陷**：对外可见的注册（副作用）发生在本地资源就绪之前，失败路径没有回滚。正确顺序应是"资源就绪 → 再注册"，或失败时显式补偿。
2. **shutdown 守卫过严**：`!started` 守卫的意图是幂等防重入，但把"启动半途失败"这个最需要清理的场景也挡在门外。清理注册表记录不依赖 gRPC 宿主是否运行，本可无条件执行。
3. **无 JOINING 超时机制兜底**：成员服务对长期停留在 JOINING 的节点没有超时摘除逻辑（审查确认 `MembershipService` 的故障判定基于 `lastHeard` 心跳，JOINING 孤儿若从未心跳，处置路径依赖实现细节，不能视为可靠兜底）。

### 6.5 修复建议

两处修改：

1. **start 失败补偿**（首选保持现顺序，便于随机端口回写）：

```java
public synchronized void start() throws IOException {
    ...
    registry.register(NodeInfo.of(self, host, configuredGrpcPort, NodeState.JOINING, 0L, weight, initialSnapshot));
    try {
        if (!grpcHost.isRunning()) {
            grpcHost.start(configuredGrpcPort);
        }
        ...自检与 ACTIVE 推进...
    } catch (IOException | RuntimeException e) {
        try {
            registry.tombstone(self);        // 失败即摘除，不留孤儿
        } catch (RuntimeException suppressed) {
            e.addSuppressed(suppressed);
        }
        throw e;
    }
    ...
}
```

2. **shutdown 放宽守卫**，把注册表清理移出 `started` 条件：

```java
public synchronized void shutdown() {
    if (!shutdown.compareAndSet(false, true)) {
        return;
    }
    ... // LEAVING / 停心跳 / 停宿主（各自判空）
    registry.tombstone(self);                // 无论 start 是否成功都执行
}
```

3. 建议增加 JOINING 超时：成员服务对超过 `2 × 心跳间隔` 仍处 JOINING 的节点自动 tombstone，作为最后一道兜底。

---

## 7. 附加风险观察（未单独复现，建议纳入整改清单）

| # | 观察 | 位置 | 风险 |
|---|------|------|------|
| A1 | 迁移完成后**源端分区数据不清理** | `MigrationEngine` / cutover 后流程 | 源节点保留旧主分区数据与元信息；若放置表回退或 cutover 语义被误解，旧数据可被再次当主读取，产生脏读；且空间只增不减 |
| A2 | 扩容重平衡（步骤 f）**不经过 freeze**，迁移期间存在写丢失窗口 | `ReconcileLoop.reconcile` 步骤 (f) 与下线路径步骤 (e)（有 freeze）不对称 | 分区在 COPYING→CUTOVER 间收到的写，cutover 后归属新主，旧主上的新写若未同步则丢失 |
| A3 | `GrpcNodeCommandChannel` 的 channels 缓存**只增不减** | `GrpcNodeCommandChannel` | 节点下线/替换后 ManagedChannel 永不关闭，连接与内存随集群历史节点数线性泄漏；长期运行的 leader 尤其明显 |
| A4 | `MembershipService.stop()` 对 heartbeatExecutor 只 `shutdownNow` 不 `awaitTermination` | `MembershipService.stop` | 停机与在途心跳任务竞态；与 `eventDispatcher` 有等待的逻辑不对称 |

---

## 8. 复现测试汇总与运行方式

| 缺陷 | 测试类 | 模块 | 运行结果 |
|------|--------|------|----------|
| 1 | `WatcherDispatchIteratorBugReproTest` | membership | FAILURE（0/2 事件送达，缺陷成立） |
| 2 | `CommandIdempotencyDeadlockBugReproTest`（2 用例） | scheduler | FAILURE × 2（缺陷成立） |
| 3 | `DeadNodeHealingGateBugReproTest` | scheduler | FAILURE（17/256 分区滞留 DEAD 节点） |
| 4 | `LeaseSplitBrainBugReproTest` | scheduler | SUCCESS（证明型：双 leader 窗口与双侧副作用证据链成立） |
| 5 | `CapabilityManagerTransitionRaceReproTest` | capability | FAILURE（start 钩子执行 2 次） |
| 6 | `NodeRuntimeStartFailureOrphanReproTest` | cluster | FAILURE（shutdown 后仍 JOINING） |

一键全量运行（PowerShell）：

```powershell
cd f:\ww\ww
.\mvnw.cmd -pl runtime/membership,runtime/capability,runtime/cluster,runtime/scheduler test "-Dtest=WatcherDispatchIteratorBugReproTest,CapabilityManagerTransitionRaceReproTest,NodeRuntimeStartFailureOrphanReproTest,CommandIdempotencyDeadlockBugReproTest,DeadNodeHealingGateBugReproTest,LeaseSplitBrainBugReproTest" "-Dsurefire.failIfNoSpecifiedTests=false"
```

> 注 1：测试全部使用内存仓库 + `TestClock`，无需 Docker / 外部依赖，单机即可运行。
> 注 2：除缺陷 4 为证明型测试（缺陷存在时通过）外，其余测试断言**期望行为**，缺陷存在时失败；修复后 1/2/3/5/6 应转绿，4 需反转断言语义后纳入回归。

## 9. 整改优先级建议

1. **P0**（正确性根基）：缺陷 4 租约 fencing（脑裂可损坏数据）→ 缺陷 3 治愈门控（故障不自愈）→ 缺陷 2 命令幂等（收敛僵死）；
2. **P1**：缺陷 1 watcher 分发（成员视图静默陈旧）→ 缺陷 5 状态机原子性（钩子重复执行）；
3. **P2**：缺陷 6 启动孤儿 + 附加观察 A1–A4。

缺陷 2/4/5 彼此耦合放大（不幂等命令 × 双 leader × 竞态跃迁 = 最坏组合），建议作为一个整改批次统一落地，并以本文 6 个复现测试作为验收基线。
