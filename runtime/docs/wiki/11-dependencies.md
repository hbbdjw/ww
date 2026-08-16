# 11. 依赖关系 — 模块依赖图与分层铁律

> 本文覆盖：runtime 家族 9 个模块的 Maven 依赖图、分层铁律与依赖门禁、第三方依赖管控、与 ww 仓库既有系统（common/admin）的关系、以及模块间的**代码级耦合点**（端口与适配）。

## 1. Maven 模块依赖图

依据各模块 `pom.xml` 的实际声明（`runtime-*` 之间均为 `compile` 依赖）：

```
                 ┌──────────────────────────────────────────────────┐
                 │                 runtime-server                    │
                 │   （唯一允许 Spring 的组装层；依赖全部 8 个模块）   │
                 └───┬───────┬───────┬───────┬───────┬───────┬──────┘
                     │       │       │       │       │       │
        ┌────────────▼──┐ ┌──▼─────────▼───┐ ┌▼───────▼───────▼────────┐
        │ runtime-      │ │ runtime-       │ │ runtime-                 │
        │ membership    │ │ scheduler      │ │ migration                │
        │               │ │                │ │                          │
        │ deps:         │ │ deps: common   │ │ deps: common proto       │
        │  common proto │ │  proto cluster │ │  placement capability    │
        └───────┬───────┘ │  capability    │ └────┬──────────┬──────────┘
                │         │  placement     │      │          │
                │         └─┬────┬────┬────┘      │          │
                │           │    │    │           │          │
        ┌───────▼───────┐   │    │    │   ┌───────▼─────┐ ┌──▼──────────┐
        │ runtime-      │   │    │    │   │ runtime-    │ │ runtime-    │
        │ cluster       │   │    │    │   │ placement   │ │ capability  │
        │               │   │    │    │   │             │ │             │
        │ deps:         │   │    │    │   │ deps:       │ │ deps:       │
        │  common proto │   │    │    │   │  common     │ │  common     │
        └───────┬───────┘   │    │    │   └──────┬──────┘ └──────┬──────┘
                │           │    │    │          │               │
                └───────────▼────▼────▼──────────▼───────────────▼┘
                              ┌──────────────────┐   ┌──────────────────┐
                              │ runtime-common   │   │ runtime-proto    │
                              │ （零依赖基石）    │   │ （仅 grpc/protobuf│
                              └──────────────────┘   │  第三方依赖）     │
                                                     └──────────────────┘
```

依赖矩阵（行依赖列，● = 直接声明依赖）：

| ↓ 依赖 \ 被依赖 → | common | proto | cluster | membership | capability | placement | scheduler | migration |
|---|---|---|---|---|---|---|---|---|
| runtime-common | — | | | | | | | |
| runtime-proto | | — | | | | | | |
| runtime-cluster | ● | ● | — | | | | | |
| runtime-membership | ● | ● | | — | | | | |
| runtime-capability | ● | | | | — | | | |
| runtime-placement | ● | | | | | — | | |
| runtime-scheduler | ● | ● | ● | | ● | ● | — | |
| runtime-migration | ● | ● | | | ● | ● | | — |
| runtime-server | ● | ● | ● | ● | ● | ● | ● | ● |

要点：

- **common 与 proto 是零模块依赖的基石**（proto 仅依赖 grpc/protobuf 第三方库）。
- **capability 与 placement 互不依赖**——二者在 scheduler/migration 处汇合；capability 需要的放置视图经 `PlacementView` 端口由 server 适配（见 §4）。
- **scheduler 与 migration 互不依赖**——scheduler 经 `MigrationCoordinator` 端口调用迁移，由 server 适配。
- **membership 不依赖 cluster**（心跳所需的节点目录/状态回写均为自身端口 `PeerDirectory`/`StateSink`，由 server 装配时用 NodeRegistry 实现）。

## 2. 分层铁律与依赖门禁

架构文档（`docs/architecture.md` §2）规定的依赖方向铁律：

```
server → {各核心模块} → {proto, common}；任何模块不得反向依赖 server
```

具体约束（分层）：

| 层 | 模块 | 约束 |
|---|---|---|
| 组装层 | server | 唯一允许 Spring；聚合一切；无人依赖它 |
| 编排层 | scheduler、migration | 可依赖下述所有层 |
| 领域层 | cluster、membership、capability、placement | 彼此几乎正交（仅 cluster 依赖 proto/common 基建） |
| 基建层 | proto、common | 零模块依赖；不依赖任何上层 |

补充门禁：

- 核心模块（server 以外）**零 Spring 依赖**——保证未来可脱离 Spring 独立运行（蓝图 §6）。
- 控制面元数据只走 PostgreSQL；数据面大数据只走 gRPC Stream——**禁止**把数据库当大数据搬运通道。
- 管理面 API 只写 Desired State，禁止直接 UPDATE actual。

## 3. 第三方依赖管控

| 依赖 | 版本 | 管控位置 | 说明 |
|---|---|---|---|
| spring-boot-starter-parent | 4.0.7 | 根 pom `ww` 的 parent | 全仓 BOM 基线 |
| grpc-bom | 1.62.2 | 根 pom `dependencyManagement` | 统一 grpc-java 全家桶 |
| protobuf-java | 3.25.5 | 根 pom `properties` | `runtime.v1` 协议 |
| netty-bom | **4.1.100.Final** | **runtime-parent `dependencyManagement` 钉版** | 覆盖 Boot 4 BOM 管理的 4.2.x——grpc-netty 1.62.x 需要 Netty 4.1.x，4.2.x 会破坏 HTTP/2 流式传输（HPACK 解码错误/巨型消息） |
| postgresql | Boot BOM 管理 | server（runtime scope） | JDBC 驱动 |
| spring-boot-starter-web / jdbc | Boot BOM 管理 | 仅 server | REST 端点 + HikariCP |

> **Netty 钉版是易踩的坑**：runtime-parent 的 `dependencyManagement` 声明在继承的 Spring Boot 依赖管理之后，覆盖全部 runtime-* 模块。升级 Boot 版本时需复核此钉版是否仍必要。

## 4. 模块间代码级耦合点（端口与适配）

Maven 依赖之外，模块协作的**全部接缝**收敛为少量接口（端口），实现由 server 装配时适配——这是「模块可独立演进」的关键：

| 接缝 | 端口（接口） | 提供方 | 消费方 | server 侧适配 |
|---|---|---|---|---|
| 放置视图 | `capability.PlacementView` | placement（数据在 `PlacementStore`） | capability（fencing 校验） | `PlacementViewAdapter` |
| 迁移协调 | `scheduler.MigrationCoordinator` | migration（`MigrationEngine`） | scheduler（drain 时迁移） | `RuntimeMigrationCoordinator` |
| 节点命令 | `scheduler.NodeCommandChannel` | gRPC 远程 + 本地回退 | scheduler（下发启停/冻结） | `GrpcNodeCommandChannel`（self 走 `LocalNodeCommandChannel`） |
| 成员目录 | `membership.PeerDirectory` / `StateSink` | cluster（`NodeRegistry`） | membership（gossip 心跳） | `RuntimeAssembly` 装配时 lambda 适配 |
| gRPC 宿主 | `proto.GrpcServerHost` | proto | membership/migration/cluster-control/health 各 Server | `RuntimeBootstrapper` 统一注册顺序 |
| 迁移数据面 | `migration.PartitionDataAdapter` | capability（MockCapability） | migration（分片导出/导入） | `MockPartitionAdapter` |
| 期望态写入 | `scheduler.DesiredStateWriter` | scheduler | server Admin API / Bootstrapper | 直接 Bean |

## 5. 与 ww 仓库既有系统的关系

根 pom `ww` 聚合三个模块：`common`、`admin`（既有系统）与 `runtime`（本 Wiki 主体）。

- **路径 C 策略**（新骨架 + 器官移植）：runtime 是全新骨架，不侵入既有代码；后续阶段以 runtime-capability 的 Capability SPI 包装既有资产（`SimpleCache`、`ObjectStore`/`Drive`/`BucketStore`、`FailoverManager` 探活、gossip 心跳逻辑等），逐步移植为 CACHE/STORAGE 真实能力。
- **当前零代码依赖**：runtime-* 不依赖 `com.ww:common`/`com.ww:admin`，反之亦然——`.\mvnw.cmd -pl common,admin -am test` 全绿验证了零回归。
- **共享的只有构建基线**：根 pom 的 Boot 4.0.7 parent、JDK 21、grpc/protobuf 版本与编码约定。

## 6. 循环依赖的规避设计

三个潜在的循环依赖点及其解法：

1. **capability ⇄ placement**（能力需要知道分区归谁 / 放置需要知道能力拓扑）：
   capability 定义 `PlacementView` 端口只返回 `Owner(node, epoch)`，不依赖 placement 的任何类；server 用 `PlacementViewAdapter` 桥接。
2. **scheduler ⇄ migration**（调度要触发迁移 / 迁移完成要更新放置）：
   scheduler 定义 `MigrationCoordinator` 端口；migration 只依赖 placement（`cutover` 切主），不依赖 scheduler。
3. **membership ⇄ cluster**（心跳需要节点目录 / 注册表需要心跳判活）：
   membership 自持 `PeerDirectory`/`StateSink` 端口与故障检测状态机，cluster 的 `NodeRegistry` 仅是目录实现之一（server 装配时注入）。

## 7. 版本与构建坐标

| 模块 | groupId:artifactId | version |
|---|---|---|
| 聚合 | `com.ww:runtime-parent`（packaging=pom） | 1.0-SNAPSHOT |
| 各子模块 | `com.ww:runtime-{common,proto,cluster,membership,capability,placement,scheduler,migration,server}` | 1.0-SNAPSHOT |

- 构建顺序由 Maven Reactor 从依赖图推导：common/proto → cluster/membership/capability/placement → scheduler/migration → server。
- **单模块构建陷阱**（phase-record §9.3 实测）：`-f runtime/server` 单模块构建从 `~/.m2` 解析兄弟模块快照；改了兄弟模块源码而未 `install` 时会用到旧 jar。规范：改兄弟模块后先 `mvnw -f runtime install`，或始终用 `-pl server -am`（连带构建依赖）。
