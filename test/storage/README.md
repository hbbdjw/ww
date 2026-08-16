# ww storage 分布式对象存储压测脚本

基于 **JMeter 5.6.3** 的 `.jmx` 压测脚本，针对 `admin` 模块暴露的 S3 兼容对象存储 HTTP API（挂载 `/storage`），
对多节点集群（默认 node1/node2/node3）进行存储桶、文件上传、下载、删除、查询等基本功能压测。

## 文件清单

| 文件 | 说明 |
| --- | --- |
| `storage-loadtest.jmx` | JMeter 测试计划（可用 JMeter 5.6.3 GUI 打开） |
| `nodes.csv` | 多节点轮询数据源（3 行 `host,port`，施压阶段请求循环轮询） |

## 压测流程

脚本严格按「**清空存储数据 → 施压 → 结果验证**」三段顺序执行（setUp → 主线程组 → tearDown），并已勾选「顺序执行线程组」（`TestPlan.serialize_threadgroups=true`），保证 **上传 → 混合读写 → 删除** 三个施压线程组严格串行：随机下载/HEAD 只会在全部文件上传完成后才发起，不会访问尚未上传的文件。

1. **setUp（清空）**：登录获取 Bearer Token → 递归删除 `BUCKET_COUNT` 个测试桶（容忍不存在）→ 重建 `BUCKET_COUNT` 个测试桶（桶名 `{BUCKET}_1`…`{BUCKET}_N`）。
2. **施压**：
   - 上传：并发上传 `FILE_COUNT` 个文件，key 为全数字零填充序号 + `.jpg` 后缀（如 `0000000001.jpg`），按「序号 → 桶」轮询（`((seq-1) % BUCKET_COUNT) + 1`）均匀分布到 N 个桶。
   - 混合读写：随机下载、HEAD 元信息查询（按同序号轮询公式定位桶）、list 随机桶列举。
   - 删除：删除序号 `1..DELETE_COUNT` 的文件（按同公式定位桶）。
3. **tearDown（验证）**：
   - 遍历 `BUCKET_COUNT` 个桶读取各自 `objectCount` 并累加，比对「累加总数 == 上传成功数 - 删除成功数」（不一致则标记失败）。
   - 对保留区间（`DELETE_COUNT+1 .. FILE_COUNT`）随机抽样下载（按同公式定位桶），断言 HTTP 200，验证「随机请求已上传文件均成功」。

## 覆盖的功能

| 功能 | 端点 | 阶段 |
| --- | --- | --- |
| 存储桶创建/删除/清空 | `PUT/DELETE /storage/buckets/{bucket}` | setUp |
| 文件上传 | `PUT /storage/buckets/{bucket}/objects/{key}`（流式） | 施压 |
| 文件下载 | `GET /storage/buckets/{bucket}/objects/{key}` | 施压 + 验证 |
| 文件删除 | `DELETE /storage/buckets/{bucket}/objects/{key}` | 施压 |
| 文件查询（元信息） | `HEAD /storage/buckets/{bucket}/objects/{key}` | 施压 |
| 文件查询（列举） | `GET /storage/buckets/{bucket}/objects?prefix=&maxKeys=` | 施压 |
| 桶统计（验证用） | `GET /storage/admin/buckets/{bucket}/stats` | 验证 |

## 关键参数（Test Plan → 用户定义的变量）

在 JMeter GUI 中打开后可直接修改，无需编辑脚本：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `NODE1_HOST` / `NODE1_PORT` | `127.0.0.1` / `8081` | 节点 1 地址 |
| `NODE2_HOST` / `NODE2_PORT` | `127.0.0.1` / `8082` | 节点 2 地址 |
| `NODE3_HOST` / `NODE3_PORT` | `127.0.0.1` / `8083` | 节点 3 地址 |
| `USERNAME` / `PASSWORD` | `admin` / `admin123` | 登录账号（默认管理员，拥有 `storage:*` 权限） |
| `BUCKET` | `loadtest` | 压测桶名前缀（实际桶名为 `{BUCKET}_1` … `{BUCKET}_N`，勿与业务桶重名） |
| `BUCKET_COUNT` | `3` | 测试桶数量 N（setUp 创建/清空 N 个桶，上传轮询分布到 N 个桶） |
| `FILE_COUNT` | `1000` | 上传文件总数（每个文件的 key 为全数字零填充序号 + `.jpg` 后缀，内容为同一个实际文件） |
| `UPLOAD_FILE` | `F:\tup\photo_2022-05-05_21-27-28.jpg` | 实际上传文件路径（每次上传读取该文件字节作为请求体） |
| `DELETE_COUNT` | `100` | 删除文件数（须 ≤ `FILE_COUNT`） |
| `KEY_DIGITS` | `10` | 文件序号零填充位数（默认 10 位） |
| `UPLOAD_THREADS` | `10` | 上传并发线程数 |
| `MIX_THREADS` / `MIX_LOOPS` | `10` / `100` | 混合读写并发线程数 / 每线程循环次数 |
| `DEL_THREADS` | `5` | 删除并发线程数 |
| `VERIFY_SAMPLES` | `20` | 验证阶段随机下载抽样次数 |

> **注意**：`KEY_DIGITS` 默认 10，序号计数器（`CounterConfig.format`）与三个随机 key 生成脚本（JSR223）均按该位数零填充。
> 若修改 `KEY_DIGITS`，请同步修改两个计数器（上传/删除）的 `CounterConfig.format` 为对应数量的 `0`。
> `DELETE_COUNT` 须 `≤ FILE_COUNT`，否则验证阶段随机下载区间为空。

## 使用方式

### GUI 打开（可视化）

1. 安装并启动 JMeter 5.6.3。
2. `File → Open`，选择 `jmeter/storage-loadtest.jmx`。
3. 在「用户定义的变量」中按需修改节点地址、账号、规模参数。
4. 点击绿色「启动」按钮运行；用「察看结果树」「汇总报告」查看请求明细与断言结果。

### 命令行（非 GUI）

```bash
# 在 jmeter/ 目录下执行（nodes.csv 使用相对路径，须与 jmx 同目录）
jmeter -n -t storage-loadtest.jmx -l result.jtl -e -o report
```

- `-n`：非 GUI 模式；`-l result.jtl`：保存结果；`-e -o report`：生成 HTML 报告。
- 结果验证结论（`objectCount`、上传成功数、删除成功数、期望数、是否一致）打印在 `jmeter.log` 与控制台。

## 多节点说明

- 施压阶段（上传/下载/HEAD/list/删除）通过 `nodes.csv` + `CSV Data Set Config`（`shareMode.all` + 循环读取）把请求循环分发到 3 个节点。
- setUp / tearDown 固定打 node1（`NODE1_HOST:NODE1_PORT`）：登录签发 token、清空/重建桶、读取桶统计。token 经集群共享缓存跨节点有效。
- 若集群只有 1~2 个节点，删减 `nodes.csv` 行即可；若节点地址变化，改 `nodes.csv` 与变量中的 `NODE*_HOST/PORT`。

## 认证说明

- `/storage/**` 受 `DualAuthFilter` 保护，需 `Authorization: Bearer <token>`；脚本 setUp 阶段调用 `/auth/login` 获取 token 并写入 JMeter 属性 `TOKEN`，全脚本通过全局请求头携带。
- 验证用管理端点 `/storage/admin/**` 额外要求 `storage:*` 权限，默认账号 `admin/admin123`（`DataInitializer` 引导，拥有 `*:*`）满足。

## 结果判定

- **文件数一致性**：`tearDown` 中 JSR223 采样器比对 `objectCount` 与「上传成功数 − 删除成功数」，不一致时该采样器标记失败并在响应消息中给出明细。
- **随机下载验证**：保留区间随机抽样 `VERIFY_SAMPLES` 次，每次断言 HTTP 200，任一失败即体现在结果树/报告中。
- 上传/删除/查询（登录/建桶/list）等 JSON 管理类接口用 JSR223 断言校验响应体包含 `"type":"success"`，下载/HEAD 用响应断言校验 HTTP 200；任一失败会在聚合报告中体现。

## 注意事项 / 验证边界

- 本机未安装 JMeter，脚本已通过 XML well-formed 与元素结构校验（元素齐全、无遗留占位符），最终请在实际 JMeter 5.6.3 GUI/命令行下加载运行确认。
- 验证阶段随机下载假设上传阶段基本全成功；若上传存在失败（序号在保留区间内），对应随机下载会 404 并计入失败，属预期暴露问题。
- 删除阶段删除的是序号 `1..DELETE_COUNT`（最早一批），删除失败不改变文件数等式（删除成功数按响应体 `"type":"success"` 计数）。
- 上传为实际二进制文件（默认 `UPLOAD_FILE` 指向的 jpg）：JSR223 前置处理器读取文件字节并以 ISO-8859-1 转成请求体字符串，上传请求 `contentEncoding=ISO-8859-1` 保证字节无损还原，`Content-Type: image/jpeg` 记录到对象元数据；换文件只需改 `UPLOAD_FILE` 变量（Windows 路径可用 `\` 或 `/`）。
