# runtime-migration 模块

> 包路径：`com.ww.runtime.migration` ｜ 源码：`runtime/migration/src/main/java/com/ww/runtime/migration/`（13 个主类）
> 定位：**分区迁移——数据流传输与任务控制（骨架）**：数据面搬运 + 切主。

## 1. 模块职责

- 驱动单个分区迁移任务走完 **10 态状态机**（PENDING→…→COMPLETED）；
- 完成源节点导出 → 目标节点导入 → 放置切主（cutover CAS）全流程；
- 支持**断点续跑**（checkpoint 持久化）、**SHA-256 checksum 校验**、**限流**、**重试与取消**；
- gRPC 传输：FetchPartition（服务端流）+ TransferStream（双向流，ack 重发）；
- 存储无关：`PartitionDataAdapter` SPI 隔离具体存储（Mock/Remote/未来 cache-storage）。

**不依赖 scheduler 模块**：以 `migrateAndWait` 提供 scheduler 期望的语义，宿主层用 `RuntimeMigrationCoordinator` 对接。

## 2. Maven 坐标与依赖

- 坐标：`com.ww:runtime-migration:1.0-SNAPSHOT`
- 依赖：`runtime-common`、`runtime-proto`（MigrationGrpc 等）、`runtime-placement`（PlacementStore/PartitionMove/Partitioner）、`runtime-capability`（MockCapability，仅 MockPartitionAdapter 用）、slf4j-api。
- 构建门禁：enforcer 禁 Spring/JPA/Caffeine。

## 3. 关键类详解

### 3.1 MigrationStateMachine（10 态状态机）

合法跃迁表（任何状态写入前必须经 `check` 校验）：

| from | 允许 to |
|---|---|
| PENDING | PREPARING, CANCELLED |
| PREPARING | SNAPSHOT, FAILED, CANCELLED |
| SNAPSHOT | COPYING, FAILED, CANCELLED |
| COPYING | SYNCING, FAILED, CANCELLED |
| SYNCING | CUTOVER, FAILED, CANCELLED |
| CUTOVER | VERIFYING, FAILED（**切主已生效，不可再取消**） |
| VERIFYING | COMPLETED, FAILED |
| COMPLETED | （终态，无出边） |
| FAILED | **PENDING（唯一终态出边：重试）** |
| CANCELLED | （终态，无出边） |

方法：`check(from,to)`（非法抛 `IllegalStateTransitionException`）、`canTransition`（不抛版）、`isTerminal`（COMPLETED/FAILED/CANCELLED）。

### 3.2 MigrationTask（record）

| 字段 | 说明 |
|---|---|
| taskId | BIGSERIAL 自增 |
| capability / partition | 能力 + 分区 |
| fromNode | 源节点（**可 null**：目标集新增分区无历史归属） |
| toNode | 目标节点 |
| state | MigrationTaskState |
| checkpointOffset / itemsDone | 断点偏移（条目数）/ 已完成条目 |
| checkpointJson | 断点元数据 `{"epoch":E,"sourceCount":S}` |
| updatedAtMillis | 更新时间 |

派生：`withState(...)`、`withProgress(...)`（checkpointJson 传 null 保留原值）。

### 3.3 MigrationTaskStore（接口）与两实现

| 方法 | 语义 |
|---|---|
| `long create(capability, partition, from, to)` | 建任务（PENDING） |
| `get / listActive / listAll` | listActive = 非终态按 taskId 升序 |
| `boolean transition(taskId, newState)` | **状态机校验 + CAS 写**：`UPDATE ... WHERE task_id=? AND state=?`（WHERE 即 CAS，命中 0 行抛 MigrationException） |
| `saveProgress(taskId, offset, items, json)` | 持久化断点（不改状态）；json 传 null 保留原值 |

- 内存版：synchronized + `tasks.replace(id, current, updated)` CAS。
- JDBC 版：create 用 `INSERT ... RETURNING task_id`；saveProgress 用 `COALESCE(?, checkpoint_json)` 实现传 null 保留。

### 3.4 MigrationEngine（编排引擎，存储无关）

字段：store、adapter（PartitionDataAdapter）、placementStore、config、clock；常量 `MAX_IMPORT_ATTEMPTS=3`、`MAX_SYNC_ROUNDS=10_000`。

| 方法 | 行为 |
|---|---|
| `boolean migrateAndWait(capability, moves)` | 每 move `obtainTask`（**活动任务复用**，不重复建）→ `runAll`（**虚拟线程池** `newVirtualThreadPerTaskExecutor` + Semaphore 限并发）→ 全部 COMPLETED 返回 true，任一 FAILED 返回 false（其余跑完） |
| `void resumeAll()` | 启动恢复：listActive 按最新 checkpointOffset 续跑；FAILED 经 FAILED→PENDING 重试跃迁后续跑（断点保留） |
| `boolean cancel(taskId)` | 仅非终态可取消 |

**runTask 状态机驱动执行流程**（按当前状态跳入，对断点续跑友好）：

```
PENDING → PREPARING : transition
PREPARING  : resolveEpoch —— 校验 placement 当前 owner == fromNode（from=null 仅读 epoch）
             返回该 epoch 作为 cutover 的 expectedEpoch；owner 不符抛 MigrationException
PREPARING → SNAPSHOT
SNAPSHOT   : sourceCount = adapter.itemCount()
             persistProgress 写 checkpoint_json {"epoch", "sourceCount"}
SNAPSHOT → COPYING
COPYING    : copyRemaining —— 自最新断点 exportChunks(checkpoint)
             逐片 importPieceWithRetry（checksum 校验 + 幂等导入，最多 3 次）
             → saveProgress → throttle 限流睡眠
COPYING → SYNCING
SYNCING    : syncUntilQuiescent —— 反复增量拉取直至为空（防复制期间新写漏迁）
             最多 10_000 轮，不收敛抛 MigrationException
SYNCING → CUTOVER
CUTOVER    : placementStore.cutover(cap, partition, toNode, epoch) ← CAS 切主（fencing）
             失败（epoch 已被并发修改）抛 MigrationException
CUTOVER → VERIFYING
VERIFYING  : targetCount = adapter.verifyCount()
             targetCount < sourceCount 抛 MigrationException（对账失败）
VERIFYING → COMPLETED
```

任意 RuntimeException → warn → 尝试 transition(FAILED)（已终态则忽略）。

### 3.5 PartitionDataAdapter（SPI，存储防腐层）

**行格式**：`key + 0x01 + value + 0x02`（UTF-8），一个分片可含多条目。

| 方法 | 用途 |
|---|---|
| `itemCount(capability, partition)` | SNAPSHOT 源端计数 |
| `List<byte[]> exportChunks(capability, partition, fromCheckpoint)` | 自断点导出（跳过前 N 条；断点续跑依赖此语义） |
| `boolean importChunks(capability, partition, chunks)` | 幂等 upsert 导入 |
| `verifyCount(capability, partition)` | VERIFYING 目标端对账计数 |

**MockPartitionAdapter**：委托同一 `MockCapability` 实例（同进程源/目标直连）；export 按 key 字典序**一条目一分片**（断点跨重启确定性）；import 解析后走 `importPartition`（Map 语义幂等）。

**RemotePartitionAdapter**：经 gRPC 远端访问。
- 构造 `(host, port)` / `(channel)` 只读（boundTaskId=0，importChunks 不可用）；`(host, port, taskId)` 绑定任务后可经 TransferStream 写入。
- `exportChunks`：blocking stub `fetchPartition`（gzip 压缩、deadline 60s），逐片 `ChunkCodec.verify`，不符抛 MigrationException；
- `importChunks`：分片入 pending → `sendRound`（async stub transferStream，60s 超时）→ 收集 ok=false 的 ack_seq 要求重发 → 最多 3 轮；
- `itemCount/verifyCount`：全量拉取后按条目终结符计数；
- 常量：FETCH_DEADLINE 60s、ROUND_TIMEOUT 60s、MAX_RESEND_ATTEMPTS 3。
- 当前定位：拉取路径已可用（单测覆盖）；跨节点写入路径的集成验证为下一阶段（Task 11 计划项）。

### 3.6 MigrationServer（gRPC 服务端，继承 MigrationImplBase）

挂在数据持有节点上，四类 RPC：

| RPC | 行为 |
|---|---|
| `fetchPartition`（服务端流） | 按 from_checkpoint 调 localAdapter.exportChunks 分片推送；每片带 seq/fin/checksum/累计 checkpointOffset；无数据发一个空 fin 片表达结束；异常 onError(INTERNAL) |
| `transferStream`（双向流接收端） | onNext：查任务 → 终态拒绝 → 首次加载进度 → checksum 校验（不符回 ok=false 要求重发）→ importChunks（失败回 ok=false）→ 累计进度 → fin 时 saveProgress 并回最终 ack |
| `controlTask`（unary） | action=cancel 时非终态任务 transition(CANCELLED)；终态/不支持返回 false |
| `reportProgress`（客户端流） | 逐条 saveProgress；完成回 ProgressSummary |

### 3.7 ChunkCodec（分片编解码 + checksum）

- 常量：`DEFAULT_CHUNK_SIZE = 4MB`；`SEP_KV=0x01`、`SEP_ENTRY=0x02`。
- `encode(taskId, pieces)`：seq 从 0 起、仅末片 fin=true、checkpointOffset 按累计**字节数**；
- `sha256Hex(data)`：SHA-256 小写 hex；
- `verify(data, checksum)`：大小写不敏感比对；
- `countEntries(piece)`：按 0x02 计数。

### 3.8 MigrationConfig（record）

| 参数 | 默认 | 含义 |
|---|---|---|
| maxConcurrentTasks | 2 | 最大并发迁移任务数（Semaphore） |
| maxBytesPerSecond | 64MB/s | 限流目标速率 |
| chunkSize | 4MB | 分片大小 |
| ackWindow | 8 | 在途未确认分片窗口 |

限流：`throttleMillisPerChunk = chunkSize × 1000 / maxBytesPerSecond`（默认 4MB/64MBps = 62ms/片）。

### 3.9 MigrationSchema

`bootstrap` 执行 `sql/rt_migration.sql`，advisory lock `pg_advisory_lock(4001, 5)` 串行化。

## 4. 数据库表

### rt_migration_task

| 列 | 类型 | 约束/默认 | 说明 |
|---|---|---|---|
| task_id | BIGSERIAL | PK | 自增 |
| capability | VARCHAR(16) | NOT NULL | 能力 |
| partition_id | INT | NOT NULL | 分区 |
| from_node | VARCHAR(64) | nullable | 源节点（新增分区为 null） |
| to_node | VARCHAR(64) | NOT NULL | 目标节点 |
| state | VARCHAR(16) | NOT NULL DEFAULT 'PENDING' | 任务状态 |
| checkpoint_offset | BIGINT | NOT NULL DEFAULT 0 | 断点偏移（条目数） |
| items_done | BIGINT | NOT NULL DEFAULT 0 | 已完成条目 |
| checkpoint_json | TEXT | nullable | `{"epoch":E,"sourceCount":S}` |
| created_at / updated_at | BIGINT | NOT NULL | 时间戳 |

## 5. 幂等 / 重试 / 断点续跑机制汇总

| 层级 | 机制 |
|---|---|
| 任务级 | obtainTask 复用同 capability+partition+to 的活动任务 |
| 分片级 | importChunks upsert（Map 语义） |
| 单片重试 | importPieceWithRetry 最多 3 次 |
| 远端重发 | 按 ack_seq 重发最多 3 轮 |
| 状态 CAS | `UPDATE WHERE state=?` 防并发修改 |
| 断点 | SNAPSHOT 写 checkpoint_json；COPYING 每片 saveProgress；exportChunks(fromCheckpoint) 支持跳前 N 条；resumeAll 重启续跑 |

## 6. 与其他模块的关系

- 依赖 common/proto/placement/capability(Mock)；**不依赖 scheduler**；
- server 的 `RuntimeMigrationCoordinator` 把 `MigrationEngine.migrateAndWait` 适配为 scheduler 的 `MigrationCoordinator` 端口（engine 内部已调用 cutover，适配器直接委托）。

## 7. 测试覆盖

| 测试类 | 覆盖点 |
|---|---|
| `MigrationStateMachineTest` | 10×10 全矩阵合法/非法交叉验证；canTransition 与 check 一致；null 拒绝；终态检测；FAILED→PENDING 唯一终态出边 |
| `MigrationEngineTest` | 3 分区迁移 + CUTOVER epoch+1；**中断续跑对账零差**（FailOnceAt200Adapter 模拟崩溃，断点恰停 200，续跑完成 500）；cutover CAS 失败标记 FAILED 且不切主（CutoverDenyStore）；持续导入失败重试上限后 FAILED；取消 PENDING + 终态不可再取消 + resumeAll 不续跑 CANCELLED；活动任务复用 |
| `MigrationConfigTest` | 默认值/合法自定义/非正参数拒绝/限流计算（62ms、截断 0、1000ms） |
| `ChunkCodecTest` | SHA-256 已知向量（"abc"/空）；encode 的 seq/fin/checksum/累计字节偏移；空输入；null 拒绝；verify 篡改检测（大小写不敏感）；countEntries |
| `InMemoryMigrationTaskStoreTest` | 自增 id/初始 PENDING；合法跃迁+时间戳；非法跃迁抛异常且状态不变；CANCELLED 不可转 FAILED；saveProgress null 保留 json；listActive/listAll |
| `JdbcMigrationTaskStoreTest` | SQL 失败包装为 MigrationException（fake DataSource） |
| `MigrationServerTest` | gRPC 端到端（进程内随机端口）：FetchPartition 全量/断点跳过/空分区；TransferStream 导入+进度持久化/拒绝坏 checksum 后接受重发/拒绝未知任务；ControlTask 仅取消非终态；ReportProgress 逐条保存 |
| `MockPartitionAdapterTest` | 计数一致；export 字典序一条目一分片；fromCheckpoint 跳过；import 往返/幂等/拒绝畸形与外分区数据 |
