# distributed-runtime 架构图（Mermaid）

> 配套 `architecture.md` 使用。

## 1. 总体架构

```mermaid
flowchart TB
    subgraph CP [Control Plane 控制面]
        PG[(PostgreSQL<br/>元数据事实源)]
        SCH[Scheduler<br/>租约竞选 + Reconcile Loop]
        PE[Placement Engine<br/>Weighted Rendezvous]
        PG <--> SCH
        SCH <--> PE
    end

    subgraph ME [Migration / Capability / Router]
        MIG[Migration Engine<br/>状态机 + 分片流]
        CM[Capability Manager]
        RT[Router<br/>分区路由 + API 加权]
    end

    subgraph GRPC [gRPC 集群网络 数据面]
        NET{{Unary + Streaming}}
    end

    subgraph NODES [Node 集群]
        A[Node A<br/>API / STORAGE]
        B[Node B<br/>API / CACHE]
        C[Node C<br/>API / CACHE]
        D[Node D<br/>STORAGE]
    end

    SCH -->|Migration Plan| MIG
    PE -->|Placement 表| RT
    CM -->|生命周期指令| NODES
    MIG <--> NET
    RT <--> NET
    NODES <--> NET
    NET -->|心跳/注册/Watch| PG
```

## 2. 模块依赖

```mermaid
flowchart LR
    SERVER[runtime-server<br/>Spring 宿主] --> ADMIN[runtime-admin]
    SERVER --> API[runtime-api]
    ADMIN --> CAP[runtime-capability]
    API --> RT[runtime-routing]
    CAP --> RT
    RT --> PLACE[runtime-placement]
    RT --> MIG[runtime-migration]
    CAP --> MIG
    RT --> MEM[runtime-membership]
    MEM --> CL[runtime-cluster]
    PLACE --> CL
    MIG --> CL
    CL --> PROTO[runtime-proto]
    MEM --> PROTO
    CL --> COMMON[runtime-common]
    PROTO --> COMMON
```

## 3. Node 生命周期

```mermaid
stateDiagram-v2
    [*] --> JOINING : 启动/注册
    JOINING --> ACTIVE : 健康+能力+资源发现+分配
    JOINING --> DEAD : 注册失败
    ACTIVE --> SUSPECTED : 心跳超时
    SUSPECTED --> ACTIVE : 心跳恢复
    SUSPECTED --> DEAD : 超限判定
    ACTIVE --> DRAINING : DRAIN 运维指令
    DRAINING --> ACTIVE : 取消排空
    DRAINING --> LEAVING : 排空完成
    ACTIVE --> LEAVING : 直接下线
    LEAVING --> DEAD : 注销
    DEAD --> [*]
```

## 4. Capability 生命周期

```mermaid
stateDiagram-v2
    [*] --> DISABLED
    DISABLED --> STARTING : enable
    STARTING --> WARMING : 组件就绪
    WARMING --> ACTIVE : 预热完成/跳过
    STARTING --> FAILED : 启动异常
    WARMING --> FAILED
    ACTIVE --> DRAINING : disable/drain
    DRAINING --> MIGRATING : 数据迁移
    MIGRATING --> STOPPING : cutover 完成
    STOPPING --> DISABLED
    DRAINING --> STOPPING : 无数据能力
    ACTIVE --> FAILED : 运行异常
    FAILED --> DISABLED
```

## 5. Migration 状态机

```mermaid
stateDiagram-v2
    [*] --> PENDING
    PENDING --> PREPARING
    PREPARING --> SNAPSHOT
    SNAPSHOT --> COPYING
    COPYING --> SYNCING : 全量完成
    SYNCING --> CUTOVER : 增量追平+校验和通过
    CUTOVER --> VERIFYING : epoch++ 换主
    VERIFYING --> COMPLETED
    PENDING --> CANCELLED
    PREPARING --> FAILED
    COPYING --> FAILED
    SYNCING --> FAILED
    FAILED --> PENDING : 重试
    COMPLETED --> [*]
```

## 6. 排空流程（DRAIN NODE A）

```mermaid
sequenceDiagram
    participant ADM as Admin API
    participant SCH as Scheduler
    participant A as Node A(旧Owner)
    participant B as Node B(新Owner)
    participant PG as PostgreSQL

    ADM->>PG: Desired: nodeA.capability=DRAINING
    SCH->>PG: 比对 Desired/Actual
    SCH->>A: 停止新分配(Freeze)
    SCH->>B: 生成 Migration Plan(P100 A→B)
    A->>B: gRPC Stream 分片复制+校验和
    A->>B: 增量日志追平
    SCH->>PG: 事务: 校验 source epoch→update placement→epoch++→CUTOVER
    SCH->>A: 旧 epoch 写入被拒绝(Fencing)
    SCH->>B: VERIFYING 通过
    SCH->>A: 观察期后清理源端
    SCH->>PG: Actual=Desired 收敛完成
```

## 7. Split Brain 防护

```mermaid
sequenceDiagram
    participant A as 旧Owner A(隔离中)
    participant W as 写路径 OwnershipManager
    participant PG as Placement(epoch)
    participant B as 新Owner B

    Note over A,PG: 网络分区，Placement 已切换
    A->>W: 写入(epoch=15)
    W->>PG: 校验当前 owner epoch
    PG-->>W: owner=B, epoch=16
    W--xA: 拒绝(StaleOwnerException)
    B->>W: 写入(epoch=16)
    W->>PG: 校验通过
    W-->>B: 接受
```
