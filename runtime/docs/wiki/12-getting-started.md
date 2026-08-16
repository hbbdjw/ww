# 12. 构建与运行指南

> 覆盖：环境前置、本地构建、单节点运行（memory/jdbc 两种模式）、多节点组集群、HTTP 管理端点操作、黑盒验收脚本、测试执行与常见问题。

## 1. 环境前置

| 项 | 要求 | 说明 |
|---|---|---|
| JDK | 21+（`maven.compiler.release=21`） | |
| Maven | 3.9.x | 仓库自带 wrapper（`mvnw` / `mvnw.cmd`），无需本机安装 |
| PostgreSQL | 任意（测试容器用 16） | 仅 `runtime.store=jdbc` 模式需要；memory 模式零外部依赖 |
| Docker | 可选 | Testcontainers 冒烟 IT 与场景脚本自动起临时 PG；不可用时 IT 静默跳过、脚本输出 SKIPPED |

## 2. 构建

```powershell
# 全量构建（含单测）
.\mvnw.cmd -f runtime install

# 快速构建可执行 jar（跳过测试；改过兄弟模块源码后必须先刷新本地快照）
.\mvnw.cmd -f runtime -DskipTests install

# 只构建 server 及其依赖链（-am 连带构建依赖，规避 ~/.m2 旧快照陷阱）
.\mvnw.cmd -f runtime -pl server -am -DskipTests package
```

产物：`runtime\server\target\runtime-server-1.0-SNAPSHOT.jar`（Spring Boot repackage 的可执行 fat jar，主类 `com.ww.runtime.server.RuntimeServerApplication`）。

> **单模块构建陷阱**：`-f runtime/server` 单模块构建从 `~/.m2` 解析兄弟模块快照；改了兄弟模块源码而未 `install` 时会用到旧 jar（实测踩到：旧 CapabilitySchema 建不出表）。规范：改兄弟模块后先 `mvnw -f runtime install`，或始终用 `-pl server -am`。

## 3. 单节点运行

### 3.1 memory 模式（零外部依赖，最快上手）

全部仓库为进程内实现：不建 DataSource、不执行 DDL、不触碰数据库。适合本地体验与单测。

```powershell
$env:RUNTIME_STORE='memory'          # 进程内存储
$env:RUNTIME_NODE_ID='node-1'
$env:RUNTIME_NODE_GRPC_PORT='0'      # 0 = 随机端口
$env:SERVER_PORT='0'                 # 随机 HTTP 端口（固定端口可指定如 8091）
java -jar runtime\server\target\runtime-server-1.0-SNAPSHOT.jar
```

启动即完成：gRPC 绑定 → 注册 ACTIVE → 心跳线程 → leader 租约 → ReconcileLoop 首轮收敛 → MOCK 能力拉起（`mock-desired` 默认 true）。

### 3.2 jdbc 模式（真实 PostgreSQL）

先准备数据库（任一 PG 实例，建库 `rt` 与用户）：

```powershell
# 例：Docker 起一个临时 PG
docker run -d --name rt-pg -e POSTGRES_DB=rt -e POSTGRES_USER=rt -e POSTGRES_PASSWORD=rt `
       -p 55432:5432 postgres:16
```

再启动节点（环境变量覆盖 `application.yml`）：

```powershell
$env:RUNTIME_NODE_ID='node-1';      $env:SERVER_PORT='8091'
$env:RUNTIME_NODE_GRPC_PORT='9101'; $env:RUNTIME_NODE_HOST='127.0.0.1'
$env:SPRING_DATASOURCE_URL='jdbc:postgresql://127.0.0.1:55432/rt'
$env:SPRING_DATASOURCE_USERNAME='rt'; $env:SPRING_DATASOURCE_PASSWORD='rt'
$env:RUNTIME_STORE='jdbc';          $env:RUNTIME_CAPABILITIES_MOCK_DESIRED='true'
java -jar runtime\server\target\runtime-server-1.0-SNAPSHOT.jar
```

启动自检（Fail-Fast）：PG `SELECT 1` 可达 → 6 张控制面表 DDL 自动创建（`CREATE TABLE IF NOT EXISTS`，幂等）→ 注册/心跳/租约/ReconcileLoop 后台线程就绪。任一步失败即终止启动。

## 4. 多节点组集群

各节点指向**同一个 PG**，用不同的 `RUNTIME_NODE_ID` / 端口即可自动组集群（无固定 Master，leader 由租约竞选产生）：

```powershell
# node-2（另开一个终端）
$env:RUNTIME_NODE_ID='node-2';      $env:SERVER_PORT='8092'
$env:RUNTIME_NODE_GRPC_PORT='9102'; $env:RUNTIME_NODE_HOST='127.0.0.1'
$env:SPRING_DATASOURCE_URL='jdbc:postgresql://127.0.0.1:55432/rt'
$env:SPRING_DATASOURCE_USERNAME='rt'; $env:SPRING_DATASOURCE_PASSWORD='rt'
java -jar runtime\server\target\runtime-server-1.0-SNAPSHOT.jar

# node-3 同理：SERVER_PORT=8093 / RUNTIME_NODE_GRPC_PORT=9103 / RUNTIME_NODE_ID=node-3
```

集群行为：

- 节点经 gRPC 心跳互发现（gossip 视图合并），注册表互通；
- 任一节点抢到 `rt_scheduler_lease` 租约成为 leader，执行 Desired→Actual 收敛；
- 新节点加入（写 desired=ACTIVE）后 leader 重算放置环，按最小移动原则迁出分区；
- 关停 leader 后其余节点 ≤2×TTL 接管租约。

> 提示：地址一律用 `127.0.0.1` 而非 `localhost`（后者可能解析为 `::1`，而监听是 IPv4-only）。

## 5. HTTP 管理端点操作

三个节点都在监听时，任选一个节点的 REST 端口操作（管理面只写期望态）：

```powershell
# 集群状态快照（leader / 能力记录 / 放置主表 / 成员视图 / 活动迁移数）
curl http://127.0.0.1:8091/rt/status

# 全量节点视图（含 DEAD 墓碑）
curl http://127.0.0.1:8091/rt/nodes

# 让 node-3 承接 MOCK 能力（扩容：写期望态，调度器自动拉起+重平衡）
curl -X PUT http://127.0.0.1:8091/rt/nodes/node-3/capabilities/MOCK/desired `
     -H "Content-Type: application/json" -d '{"desired":"ACTIVE"}'

# 排空 node-1（下线：freeze→迁移→cutover→actual=DISABLED）
curl -X PUT http://127.0.0.1:8091/rt/nodes/node-1/capabilities/MOCK/desired `
     -H "Content-Type: application/json" -d '{"desired":"DISABLED"}'
```

## 6. 黑盒验收脚本（推荐）

一键完成「构建产物 + 真实 PG + 3 真实节点进程」的端到端验收：

```powershell
powershell -File scripts\runtime-scenario.ps1            # 默认 PG 端口 55432
powershell -File scripts\runtime-scenario.ps1 -SkipCleanup   # 保留容器/日志排查
```

脚本行为：自动探测 Docker（PATH 上的 `docker`，回退 `wsl -e docker`；不可用则全部 SKIPPED 退出 0）→ 起临时 PG 容器 `rt-scenario-pg` → 依次拉起 node-1/2/3（HTTP 8091-8093 / gRPC 9101-9103）→ 执行 7 项断言 → 清理进程与容器。日志落在 `scripts\build\runtime-scenario\`。

7 项断言（2026-08-15 实测 7/7 PASS）：

| # | 场景 | 断言 |
|---|---|---|
| 1 | 3 节点注册 | 三节点互相可见全部 ACTIVE |
| 2 | 能力只在期望节点 | leader=node-1，MOCK primaries 仅 node-1/node-2 |
| 3 | 扩容重平衡 | 启用 node-3 后其 share ∈ [20%, 45%]（收敛中，终态 ≈1/3） |
| 4 | 排空切主 | node-1 actual=DISABLED，primaries 不含 node-1 |
| 5 | 租约接管 | kill leader 后 ≤15s 接管（加速参数 TTL=5s）并继续服务 |
| 6 | 死亡检测 | 存活视角 SUSPECTED ≤15s，重算后 primaries 排除 ≤60s |
| 7 | 陈旧纪元 fencing | 由单测 MockCapabilityTest 4 用例保证（打印参考） |

注意：脚本使用**加速时序参数**（心跳 500ms / suspect 2 / dead 4 / TTL 5s），仅用于验收提速；生产默认值见 `RuntimeProperties`（心跳 1s / TTL 10s / 冷却 30s）。

## 7. 测试

```powershell
# 全部单测（43 个测试类；Surefire 只跑 *Test）
.\mvnw.cmd -f runtime test

# Testcontainers PG 冒烟 IT（Failsafe 在 integration-test/verify 阶段跑 *IT）
.\mvnw.cmd -f runtime/server clean verify

# 既有系统零回归检查
.\mvnw.cmd -pl common,admin -am test
```

| 套件 | 说明 |
|---|---|
| 单测 43 类 | 状态机合法/非法跃迁、租约互斥与过期接管、HRW 最小移动、并发 cutover、迁移断点续跑对账为零差、校验和不匹配重试、旧 epoch 拒写、ReconcileLoop 编排序列等 |
| `RuntimePostgresSmokeIT` | 真实 PG 16 容器，5 用例：建表、注册、desired/actual 往返、placement 原子替换、租约互斥 |
| `RuntimeServerIntegrationTest` | `store=memory` 全装配集成测试，无需 PG |
| 场景脚本 | 见 §6 |

Testcontainers 环境注意（Windows + WSL dockerd）：本机 dockerd 在 WSL 内时，需 `scripts/runtime-docker-bridge.py` 桥 + `$env:DOCKER_HOST='tcp://127.0.0.1:2375'`；未设置时 IT 以 `disabledWithoutDocker` 静默跳过。

## 8. 常见问题

| 现象 | 原因与处理 |
|---|---|
| 启动失败：PostgreSQL self-check failed | PG 不可达或凭据错误；检查 `SPRING_DATASOURCE_*` 与 PG 是否监听 IPv4（用 `127.0.0.1` 而非 `localhost`） |
| 建表失败 / 行为与源码不符 | 单模块构建用到了 `~/.m2` 旧兄弟模块快照；重跑 `.\mvnw.cmd -f runtime install` 或用 `-pl server -am` |
| gRPC 端口冲突 | `RUNTIME_NODE_GRPC_PORT=0` 用随机端口（单测做法），或给每个节点分配独立端口 |
| 多节点互相看不见 | 各节点未指向同一 PG，或 `RUNTIME_NODE_HOST` 配成了对端不可达的地址 |
| 场景脚本全 SKIPPED | Docker 不可用（探测 `docker` 与 `wsl -e docker` 均失败）；脚本按设计退出 0，不算失败 |
| HTTP/2 流式传输异常（HPACK/巨型消息） | Netty 版本被抬高破坏钉版；确认 runtime-parent 的 netty-bom 4.1.100.Final 生效 |
| kill 节点后很久才接管/重算 | 生产默认时序较保守（TTL 10s、suspect 3、dead 6）；可按需调小 `RUNTIME_SCHEDULER_TTL_MS` 等 |

## 9. 配置速查（环境变量 → 配置项）

完整表见 [10-server.md](10-server.md) §3。高频项：

| 环境变量 | 默认 | 说明 |
|---|---|---|
| `RUNTIME_NODE_ID` / `RUNTIME_NODE_HOST` / `RUNTIME_NODE_GRPC_PORT` / `RUNTIME_NODE_WEIGHT` | node-1 / localhost / 9100 / 1.0 | 节点标识与地址 |
| `RUNTIME_STORE` | jdbc | jdbc / memory |
| `SPRING_DATASOURCE_URL` / `_USERNAME` / `_PASSWORD` | localhost:5432/ww | PG 连接（仅 jdbc 模式） |
| `SERVER_PORT` | 8080 | REST 管理端口 |
| `RUNTIME_HEARTBEAT_INTERVAL_MS` / `RUNTIME_SUSPECT_AFTER_MISSES` / `RUNTIME_DEAD_AFTER_MISSES` | 1000 / 3 / 6 | 成员关系时序 |
| `RUNTIME_SCHEDULER_TTL_MS` / `_RENEW_INTERVAL_MS` / `_RECONCILE_INTERVAL_MS` | 10000 / 3000 / 2000 | 调度时序 |
| `RUNTIME_PARTITION_COUNT` / `RUNTIME_REPLICATION_FACTOR` | 4096 / 1 | 放置参数 |
| `RUNTIME_MIGRATION_MAX_CONCURRENT` / `_MAX_BYTES_PER_SECOND` / `_CHUNK_SIZE` / `_ACK_WINDOW` | 2 / 64MB/s / 4MB / 8 | 迁移参数 |
| `RUNTIME_CAPABILITIES_MOCK_DESIRED` | true | 启动即写 MOCK 期望态 ACTIVE |
