#!/usr/bin/env bash

if [ -z "${BASH_VERSION:-}" ]; then
    if command -v bash >/dev/null 2>&1; then
        exec bash "$0" "$@"
    else
        echo "ERR: 需要 bash" >&2
        exit 1
    fi
fi

set -euo pipefail
trap 'err "脚本异常退出：第 ${LINENO} 行命令失败 → ${BASH_COMMAND}"' ERR

# ============================================================
#  横向扩展脚本：向已有 runtime 集群动态添加单个节点
#
#  用法：./runtime/scale-out.sh [--mock] <节点序号> [HTTP端口]
#    --mock    — 可选，启用 MOCK 能力（默认仅禁用 CACHE/STORAGE）
#    节点序号  — 必填，如 5 表示 node-5
#    HTTP端口  — 可选，默认 8090 + 序号（如 node-5 → 8095）
#
#  前提：已通过 deploy-runtime.sh all 启动基础 4 节点集群
#  特性：新节点 CACHE/STORAGE 能力默认禁用，需在管理页面手动启用；
#        加 --mock 可同时启用 MOCK 能力。
# ============================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

IMAGE_NAME="ww-runtime:latest"
NETWORK_NAME="ww-runtime-net"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()  { echo -e "${RED}[ERR]${NC}   $*"; }

# -----------------------------------------------------------
# 参数解析
# -----------------------------------------------------------
MOCK_ENABLED=false
POSITIONAL_ARGS=()

for arg in "$@"; do
    case "$arg" in
        --mock)
            MOCK_ENABLED=true
            ;;
        -h|--help)
            echo "用法: $0 [--mock] <节点序号> [HTTP端口]"
            echo ""
            echo "选项:"
            echo "  --mock       启用 MOCK 能力（默认不启用）"
            echo ""
            echo "示例:"
            echo "  $0 5              # 启动 node-5，REST 8095，能力全禁用"
            echo "  $0 --mock 5       # 启动 node-5，MOCK 能力启用"
            echo "  $0 6 9096         # 启动 node-6，REST 9096，能力全禁用"
            echo ""
            echo "前提: 已通过 deploy-runtime.sh all 启动基础集群"
            exit 0
            ;;
        *)
            POSITIONAL_ARGS+=("$arg")
            ;;
    esac
done

if [ ${#POSITIONAL_ARGS[@]} -eq 0 ]; then
    echo "用法: $0 [--mock] <节点序号> [HTTP端口]"
    echo ""
    echo "示例:"
    echo "  $0 5              # 启动 node-5，REST 8095，能力全禁用"
    echo "  $0 --mock 5       # 启动 node-5，MOCK 能力启用"
    echo "  $0 6 9096         # 启动 node-6，REST 9096，能力全禁用"
    echo ""
    echo "前提: 已通过 deploy-runtime.sh all 启动基础集群"
    exit 1
fi

NODE_INDEX="${POSITIONAL_ARGS[0]}"
NODE_ID="node-${NODE_INDEX}"
CONTAINER_NAME="ww-rt-node${NODE_INDEX}"
SERVICE_ALIAS="rt-node${NODE_INDEX}"
HTTP_PORT="${POSITIONAL_ARGS[1]:-$((8090 + NODE_INDEX))}"
GRPC_INTERNAL_PORT=9100
GRPC_HOST_PORT=$((9100 + NODE_INDEX))

STORAGE_VOLUME="ww-storage-node${NODE_INDEX}"
CACHE_VOLUME="ww-cache-node${NODE_INDEX}"

if [ "$MOCK_ENABLED" = true ]; then
    CAPABILITY_STATUS="MOCK=启用  CACHE=禁用  STORAGE=禁用"
else
    CAPABILITY_STATUS="MOCK=禁用  CACHE=禁用  STORAGE=禁用"
fi

echo ""
log "=============================================="
log "  横向扩展：添加 ${NODE_ID}"
log "=============================================="
log "  容器名称:    ${CONTAINER_NAME}"
log "  REST 端口:   ${HTTP_PORT} (宿主机) → ${HTTP_PORT} (容器)"
log "  gRPC 端口:   ${GRPC_HOST_PORT} (宿主机) → ${GRPC_INTERNAL_PORT} (容器)"
log "  Docker 网络: ${NETWORK_NAME} (别名 ${SERVICE_ALIAS})"
log "  存储卷:      ${STORAGE_VOLUME} / ${CACHE_VOLUME}"
log "  能力状态:    ${CAPABILITY_STATUS}（需页面手动启用 CACHE/STORAGE）"
log "=============================================="
echo ""

# -----------------------------------------------------------
# 前置检查
# -----------------------------------------------------------
if ! command -v docker &>/dev/null; then
    err "docker 未安装"
    exit 1
fi

if ! docker info &>/dev/null; then
    err "Docker 未运行"
    exit 1
fi

if ! docker image inspect "$IMAGE_NAME" &>/dev/null; then
    err "镜像 ${IMAGE_NAME} 不存在，请先执行: ./runtime/deploy-runtime.sh build"
    exit 1
fi

if ! docker network inspect "$NETWORK_NAME" &>/dev/null; then
    err "网络 ${NETWORK_NAME} 不存在，请先通过 deploy-runtime.sh all 启动基础集群"
    exit 1
fi

if docker ps -a --format '{{.Names}}' | grep -qx "$CONTAINER_NAME"; then
    warn "容器 ${CONTAINER_NAME} 已存在，正在移除..."
    docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
fi

# 检查端口冲突
for port in "$HTTP_PORT" "$GRPC_HOST_PORT"; do
    if docker ps --format '{{.Ports}}' | grep -q "0.0.0.0:${port}->"; then
        err "宿主机端口 ${port} 已被占用"
        exit 1
    fi
done

# 创建数据卷（幂等）
docker volume create "$STORAGE_VOLUME" >/dev/null
docker volume create "$CACHE_VOLUME" >/dev/null

# -----------------------------------------------------------
# 启动容器
# -----------------------------------------------------------
log "启动容器 ${CONTAINER_NAME} ..."

MOCK_DESIRED="false"
if [ "$MOCK_ENABLED" = true ]; then
    MOCK_DESIRED="true"
fi

docker run -d \
    --name "$CONTAINER_NAME" \
    --mem-limit 8g \
    --network "$NETWORK_NAME" \
    --network-alias "$SERVICE_ALIAS" \
    --extra-hosts "host.docker.internal:host-gateway" \
    -p "${HTTP_PORT}:${HTTP_PORT}" \
    -p "${GRPC_HOST_PORT}:${GRPC_INTERNAL_PORT}" \
    -v "${STORAGE_VOLUME}:/app/data/storage" \
    -v "${CACHE_VOLUME}:/app/data/cache" \
    -e RUNTIME_STORE=jdbc \
    -e SPRING_DATASOURCE_URL="jdbc:postgresql://host.docker.internal:5432/ww" \
    -e SPRING_DATASOURCE_USERNAME=postgres \
    -e SPRING_DATASOURCE_PASSWORD="jw3344..." \
    -e TZ=Asia/Shanghai \
    -e RUNTIME_NODE_ID="$NODE_ID" \
    -e RUNTIME_NODE_HOST="$SERVICE_ALIAS" \
    -e RUNTIME_NODE_GRPC_PORT="$GRPC_INTERNAL_PORT" \
    -e SERVER_PORT="$HTTP_PORT" \
    -e RUNTIME_HEARTBEAT_INTERVAL_MS=500 \
    -e RUNTIME_SUSPECT_AFTER_MISSES=2 \
    -e RUNTIME_DEAD_AFTER_MISSES=4 \
    -e RUNTIME_SCHEDULER_TTL_MS=5000 \
    -e RUNTIME_SCHEDULER_RENEW_INTERVAL_MS=1000 \
    -e RUNTIME_SCHEDULER_RECONCILE_INTERVAL_MS=500 \
    -e RUNTIME_REBALANCE_COOLDOWN_MS=3000 \
    -e RUNTIME_REPLICATION_FACTOR=2 \
    -e RUNTIME_HEALING_GRACE_PERIOD_MS=30000 \
    -e RUNTIME_MIGRATION_MAX_CONCURRENT=8 \
    -e RUNTIME_CAPABILITIES_MOCK_DESIRED="$MOCK_DESIRED" \
    -e RUNTIME_CAPABILITIES_CACHE_DESIRED=false \
    -e RUNTIME_CAPABILITIES_STORAGE_DESIRED=false \
    "$IMAGE_NAME" >/dev/null

log "容器已启动 ✓"

# -----------------------------------------------------------
# 健康检查
# -----------------------------------------------------------
log "等待 ${NODE_ID} /rt/health 就绪（最多 120 秒）..."

ready=0
for _ in $(seq 1 24); do
    # 存活探针无需鉴权（/rt/status 已由 Spring Security 保护，需 Bearer token）
    code=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:${HTTP_PORT}/rt/health" 2>/dev/null || echo "000")
    if [ "$code" = "200" ]; then
        ready=1
        break
    fi
    sleep 5
done

if [ "$ready" -ne 1 ]; then
    echo ""
    err "${NODE_ID} 未能在 120 秒内就绪"
    err "查看日志: docker logs ${CONTAINER_NAME}"
    echo ""
    echo "  常见原因:"
    echo "    - PostgreSQL 不可达 → 检查 PG listen_addresses 和 pg_hba.conf"
    echo "    - 端口冲突       → 检查宿主机端口 ${HTTP_PORT} 是否被占用"
    exit 1
fi

log "${NODE_ID} 已就绪 ✓"
echo ""
if [ "$MOCK_ENABLED" = true ]; then
    echo -e "  ${GREEN}${NODE_ID} 已加入集群（MOCK 能力已启用）${NC}"
else
    echo -e "  ${GREEN}${NODE_ID} 已加入集群（能力禁用）${NC}"
fi
echo "=============================================="
echo ""
echo "  管理界面:   http://localhost:${HTTP_PORT}/   （登录账号默认 admin / admin123）"
echo "  存活探针:   http://localhost:${HTTP_PORT}/rt/health   （无需鉴权）"
echo "  REST API:   http://localhost:${HTTP_PORT}/rt/status   （需 Bearer token）"
echo "              http://localhost:${HTTP_PORT}/rt/nodes"
echo ""
echo "  下一步操作:"
echo "    1. 打开管理界面 http://localhost:${HTTP_PORT}/"
echo "    2. 在集群管理页面找到 ${NODE_ID}"
echo "    3. 手动启用 CACHE 和/或 STORAGE 能力"
echo "    4. 启用后调度器会自动分配分区并执行数据迁移"
echo ""
echo "  常用命令:"
echo "    查看日志:   docker logs -f ${CONTAINER_NAME}"
echo "    停止节点:   docker stop ${CONTAINER_NAME}"
echo "    重新启动:   docker start ${CONTAINER_NAME}"
echo "    移除节点:   docker rm -f ${CONTAINER_NAME}"
echo "    清理数据卷: docker volume rm ${STORAGE_VOLUME} ${CACHE_VOLUME}"
echo ""
