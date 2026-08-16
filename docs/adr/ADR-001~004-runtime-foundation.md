# ADR-001: Java 版本锁定 21（偏离蓝图 JDK 25）

- **状态**：已接受
- **背景**：蓝图 §5 要求 JDK 25；现仓全模块（root pom `maven.compiler.release=21`）与生产镜像均基于 Java 21；蓝图自述 Structured Concurrency 仅"适用范围内"使用，其在 25 仍是 preview 特性。
- **决策**：runtime-* 全部模块锁 Java 21。虚拟线程（21 已稳定）、Records、Sealed Classes、Pattern Matching 照常使用；Structured Concurrency 不进入核心路径。
- **后果**：正向——零工具链迁移风险、与现有部署/镜像兼容；负向——无法使用 25 的稳定化特性。
- **重新评估触发**：现仓整体升级 JDK（独立低风险项）时同步升级 runtime-*。
- **责任**：用户 + CI（`mvnw verify` 编译门禁）。

# ADR-002: gRPC 传输——typed proto 服务优先，UnifiedRpcService 保留兼容

- **状态**：已接受
- **背景**：蓝图 §7/§14 要求 runtime-proto 定义 typed .proto（cluster/membership/placement/migration...）且四种 RPC 形态齐全；现有 `common/cluster-rpc` 提供 UnifiedRpcService（单通道 + service_name + payload bytes）。
- **决策**：runtime-* 节点间通信采用 **typed proto 服务**（强类型契约、Watch/流式迁移一等公民）；复用 cluster-rpc 的**构建链与运维经验**（protobuf-maven-plugin 配置、拦截器模式）。UnifiedRpcService 不废弃——现有 cache/storage 在并存期继续使用，后续按能力逐个切换。
- **后果**：正向——契约清晰、流式语义显式；负向——并存期两套 gRPC 协议栈（端口隔离：typed=9100，legacy=9000）。
- **重新评估触发**：全部能力切换完成后的协议栈合并评审。

# ADR-003: 控制面元数据访问采用 plain JDBC（非 JPA）

- **状态**：已接受
- **背景**：蓝图 §6 要求核心模块"POJO + Java Core，能脱离 Spring 运行"；现仓控制面无先例（业务用 JPA）。§87 要求明确事务边界（Cutover 事务等）。
- **决策**：runtime 控制面存储层用 **HikariCP + plain JDBC + 手写 SQL**，事务边界显式（`setAutoCommit(false)` + commit/rollback）；不引入 JPA/实体映射。连接池仅由 runtime-server 宿主装配注入。
- **后果**：正向——核心零 Spring 数据依赖、事务/乐观锁完全可控、贴合 epoch CAS 语义；负向——无实体映射便利，SQL 手工维护。
- **重新评估触发**：若元数据查询复杂度显著上升（Join 报表类需求），仅在 runtime-admin 层引入只读 JPA，核心存储层不变。

# ADR-004: 落地路径 C——新骨架 + 器官移植

- **状态**：已接受
- **背景**：三个候选路径（A 全新建仓 / B 现仓硬改 / C 混合）详见《distributed-runtime蓝图可行性分析与改造难度评估》第四节。
- **决策**：采用路径 C——按蓝图模块结构在**现仓新增** runtime-* Maven 模块；高复用资产迁入（RendezvousHash/SimpleCache/ObjectStore/StorageMigrationService/cluster-rpc）；ww-admin 后续改造为首个宿主应用；每能力独立开关（`runtime.routing.mode=hrw|partition`）可单独回退。
- **后果**：正向——结构完整、复用 70-90%、每步可回退；负向——并存期需双路由开关与指标区分。
- **重新评估触发**：分区路由性能回退 >15%，或开关无法隔离语义冲突 → 重评路径 B。
- **Bootstrap 约定**：初始 placement 以现行加权 HRW 对 partitionId 计算，保证首切零迁移。
