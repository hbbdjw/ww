# ============================================================
# 本地 node-0 启动脚本（Windows 侧，加入 WSL Docker 中 node1-4 集群）
#
# 用法: 在 PowerShell 中执行  .\runtime\start-node0.ps1
#
# 前置条件:
#   - WSL 中 PostgreSQL 18 已启动（Windows 经 localhost:5432 访问）
#   - docker-compose 的 node1-4 已启动（RUNTIME_NODE_HOST 为 WSL eth0 IP）
#   - 已编译 jar: .\mvnw.cmd -f runtime -pl server -am -DskipTests -B clean package
#
# 注意: WSL 重启后两侧 IP 都可能变化——
#   - 容器侧: 用 `wsl hostname -I` 取新 IP，更新 docker-compose.yml 并重建容器
#   - 本侧: 脚本自动探测 vEthernet (WSL) IPv4 作为通告地址，无需手工改
# ============================================================
$ErrorActionPreference = 'Stop'

$scriptDir = $PSScriptRoot
$dataDir = Join-Path $scriptDir 'data\node0'
foreach ($d in @('cache', 'storage\d1', 'storage\d2', 'storage\d3', 'storage\d4')) {
    New-Item -ItemType Directory -Force -Path (Join-Path $dataDir $d) | Out-Null
}

# ---- JDK 21（PATH 默认 java 是 1.8，无法运行本 jar；可用 JAVA21_HOME 覆盖）----
$java = if ($env:JAVA21_HOME) { Join-Path $env:JAVA21_HOME 'bin\java.exe' } else { 'D:\jdks\corretto-21.0.8\bin\java.exe' }
if (-not (Test-Path $java)) {
    Write-Error "未找到 JDK 21: $java （可用环境变量 JAVA21_HOME 指定 JDK 目录）"
    exit 1
}

# ---- 对外通告地址 = Windows 侧 vEthernet (WSL) 网卡 IPv4（容器经默认网关回连本节点）----
if ($env:RUNTIME_NODE_HOST_OVERRIDE) {
    $env:RUNTIME_NODE_HOST = $env:RUNTIME_NODE_HOST_OVERRIDE
} else {
    $ip = (Get-NetIPAddress -InterfaceAlias 'vEthernet (WSL*' -AddressFamily IPv4 -ErrorAction SilentlyContinue |
        Select-Object -First 1).IPAddress
    if (-not $ip) {
        Write-Error '未能探测到 vEthernet (WSL) IPv4，请设置 RUNTIME_NODE_HOST_OVERRIDE=<Windows侧WSL网卡IP> 后重试'
        exit 1
    }
    $env:RUNTIME_NODE_HOST = $ip
}

# ---- 节点身份与端口 ----
$env:RUNTIME_NODE_ID = 'node-0'
$env:RUNTIME_NODE_GRPC_PORT = '9100'
$env:SERVER_PORT = '8090'
$env:RUNTIME_STORE = 'jdbc'

# ---- PostgreSQL（WSL 宿主机 PG 18；默认 ww/ww 是错的，必须用真实凭据）----
$env:SPRING_DATASOURCE_URL = 'jdbc:postgresql://localhost:5432/ww'
$env:SPRING_DATASOURCE_USERNAME = 'postgres'
$env:SPRING_DATASOURCE_PASSWORD = 'jw3344...'

$env:TZ = 'Asia/Shanghai'

# ---- 时序参数（与 docker-compose x-runtime-common 对齐）----
$env:RUNTIME_HEARTBEAT_INTERVAL_MS = '500'
$env:RUNTIME_SUSPECT_AFTER_MISSES = '2'
$env:RUNTIME_DEAD_AFTER_MISSES = '4'
$env:RUNTIME_SCHEDULER_TTL_MS = '5000'
$env:RUNTIME_SCHEDULER_RENEW_INTERVAL_MS = '1000'
$env:RUNTIME_SCHEDULER_RECONCILE_INTERVAL_MS = '500'
$env:RUNTIME_REBALANCE_COOLDOWN_MS = '3000'
$env:RUNTIME_REPLICATION_FACTOR = '2'
$env:RUNTIME_HEALING_GRACE_PERIOD_MS = '30000'
$env:RUNTIME_MIGRATION_MAX_CONCURRENT = '8'

# ---- 数据面能力（CACHE/STORAGE，与容器节点一致）----
$env:RUNTIME_CAPABILITIES_CACHE_DESIRED = 'true'
$env:RUNTIME_CAPABILITIES_CACHE_AOF_ENABLED = 'true'
$env:RUNTIME_CAPABILITIES_CACHE_DATA_DIR = Join-Path $dataDir 'cache'
$env:RUNTIME_CAPABILITIES_STORAGE_DESIRED = 'true'
$env:RUNTIME_CAPABILITIES_STORAGE_DRIVES = ((1..4 | ForEach-Object { Join-Path $dataDir "storage\d$_" }) -join ',')

# ---- jar ----
$jar = Join-Path $scriptDir 'server\target\runtime-server-1.0-SNAPSHOT.jar'
if (-not (Test-Path $jar)) {
    Write-Error "jar 不存在: $jar`n请先编译: .\mvnw.cmd -f runtime -pl server -am -DskipTests -B clean package"
    exit 1
}

Write-Host "启动 node-0: 通告地址 $($env:RUNTIME_NODE_HOST):9100, REST :8090, JDK $java" -ForegroundColor Green
& $java '-Xms1g' '-Xmx2g' '-Dfile.encoding=UTF-8' '-Duser.timezone=Asia/Shanghai' '-jar' $jar
