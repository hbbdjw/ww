#!/usr/bin/env bash
# ============================================================
# ww 后端部署脚本（3 节点集群）
# 前端需手工部署（pnpm build + nginx），本脚本仅管理后端节点
#
# 用法:
#   ./deploy.sh                无参数 = all（一键编译 + 构建 + 启动）
#   ./deploy.sh all            一键编译 + 构建镜像 + 启动集群
#   ./deploy.sh build          编译后端 + 构建后端镜像
#   ./deploy.sh up             启动 3 个后端节点（自动编译 + 构建镜像）
#   ./deploy.sh down           停止并移除全部服务
#   ./deploy.sh restart        重启全部服务
#   ./deploy.sh logs           查看所有服务日志
#   ./deploy.sh status         查看集群状态
#
# Windows 用户请在 WSL 中执行本脚本（本机无 Docker Desktop，Docker 环境位于 WSL）
# ============================================================

if [ -z "${BASH_VERSION:-}" ]; then
    if command -v bash >/dev/null 2>&1; then
        exec bash "$0" "$@"
    else
        echo "ERR: 需要 bash，但当前环境未找到 bash" >&2
        exit 1
    fi
fi

set -euo pipefail

IMAGE_NAME="ww-admin:latest"
COMPOSE_FILE="docker-compose.yml"
ADMIN_VERSION="1.0-SNAPSHOT"
JAR_FILE="admin/target/admin-${ADMIN_VERSION}.jar"

RUNTIME_BASE_IMAGE="swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/khipu/openjdk21-alpine:latest"

MVN_CMD=""
IS_WSL=0

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()  { echo -e "${RED}[ERR]${NC}   $*"; }

check_prereqs() {
    if ! command -v docker &>/dev/null; then
        err "docker 未安装，请先安装 Docker"
        exit 1
    fi
    if ! docker info &>/dev/null; then
        err "Docker 未运行，请先启动 Docker 服务"
        exit 1
    fi
    if [ ! -f "$COMPOSE_FILE" ]; then
        err "未找到 ${COMPOSE_FILE}，请在项目根目录执行本脚本"
        exit 1
    fi
}

_mvn_works() {
    local cmd="$1"
    "$cmd" -v >/dev/null 2>&1
}

check_build_env() {
    IS_WSL=0
    if grep -qi microsoft /proc/version 2>/dev/null; then
        IS_WSL=1
    fi

    MVN_CMD=""
    if [ "$IS_WSL" -eq 1 ]; then
        if [ -x "/mnt/d/maven/apache-maven-3.9.0/bin/mvn.cmd" ] && _mvn_works "/mnt/d/maven/apache-maven-3.9.0/bin/mvn.cmd"; then
            MVN_CMD="/mnt/d/maven/apache-maven-3.9.0/bin/mvn.cmd"
            log "WSL2: 使用 D 盘 Maven: $MVN_CMD"
        elif command -v mvn.cmd &>/dev/null && _mvn_works "mvn.cmd"; then
            MVN_CMD="mvn.cmd"
            log "WSL2: 使用 Windows Maven (PATH): $MVN_CMD"
        fi
    fi
    if [ -z "$MVN_CMD" ]; then
        if command -v mvn &>/dev/null && _mvn_works "mvn"; then
            MVN_CMD="mvn"
        fi
    fi

    if [ -z "$MVN_CMD" ]; then
        warn "未找到可执行的 Maven 命令"
    else
        local mvn_v_out
        mvn_v_out=$($MVN_CMD -v 2>&1 | head -3)
        log "$(echo "$mvn_v_out" | head -1)"
    fi
}

ensure_runtime_image() {
    if docker image inspect "$RUNTIME_BASE_IMAGE" &>/dev/null; then
        log "运行时基础镜像已存在: $RUNTIME_BASE_IMAGE"
    else
        warn "运行时镜像不存在，正在拉取: $RUNTIME_BASE_IMAGE"
        if ! docker pull "$RUNTIME_BASE_IMAGE"; then
            err "拉取失败: $RUNTIME_BASE_IMAGE"
            exit 1
        fi
        log "拉取完成: $RUNTIME_BASE_IMAGE"
    fi
}

# -----------------------------------------------------------
# 后端 Maven 编译
# -----------------------------------------------------------
compile_jar() {
    log "开始本地 Maven 编译打包 ..."
    if [ ! $IS_WSL -eq 1 ] && [ -f .mvn/jvm.config ]; then
        # jvm.config 中 stdout.encoding=GBK 是为 Windows cp936 控制台准备的，
        # 纯 Linux 下无效，通过 MAVEN_ARGS 覆盖为 UTF-8 避免乱码
        export MAVEN_ARGS="${MAVEN_ARGS:-} -Dstdout.encoding=UTF-8 -Dstderr.encoding=UTF-8"
    fi
    $MVN_CMD package -DskipTests -pl admin -am -B
    if [ ! -f "$JAR_FILE" ]; then
        err "编译完成但未找到 jar: $JAR_FILE"
        exit 1
    fi
    local jar_size
    jar_size=$(du -h "$JAR_FILE" | awk '{print $1}')
    log "编译完成 ✓  jar: $JAR_FILE ($jar_size)"
}

_jar_exists() {
    [ -f "$JAR_FILE" ]
}

# -----------------------------------------------------------
# 构建后端 Docker 镜像
# -----------------------------------------------------------
build_backend_image() {
    log "开始构建后端 Docker 镜像 ${IMAGE_NAME} ..."
    if [ ! -f "$JAR_FILE" ]; then
        err "jar 包不存在: $JAR_FILE"
        err "请先执行: mvn package -DskipTests -pl admin -am"
        exit 1
    fi
    docker build --pull=false -t "${IMAGE_NAME}" .
    # 清理旧版本产生的悬空镜像层，避免磁盘占用持续增长
    docker image prune -f >/dev/null 2>&1 || true
    log "后端镜像构建完成 ✓"
}

build_backend() {
    ensure_runtime_image
    check_build_env
    if [ -n "$MVN_CMD" ]; then
        compile_jar
    else
        warn "未找到可执行的 Maven 命令"
        if _jar_exists; then
            local jar_size
            jar_size=$(du -h "$JAR_FILE" | awk '{print $1}')
            log "检测到已编译的 jar（$jar_size），直接复用"
        else
            err "jar 包不存在，且当前环境无法执行 Maven"
            exit 1
        fi
    fi
    build_backend_image
}

build() {
    build_backend
}

# -----------------------------------------------------------
# 启动后端 3 节点（--skip-build 跳过构建，供 all 命令复用）
# -----------------------------------------------------------
up() {
    if [ "${1:-}" != "--skip-build" ]; then
        log "构建最新 jar 包与 Docker 镜像..."
        build_backend
    fi

    log "启动 ww 后端 3 节点集群（强制重建容器以使用最新镜像）..."
    docker compose -f "${COMPOSE_FILE}" up -d --force-recreate

    echo ""
    log "等待后端节点启动（最多 120 秒）..."

    local started=0
    for i in $(seq 1 24); do
        local ok=0
        for port in 8081 8082 8083; do
            if curl -s -o /dev/null -w "%{http_code}" "http://localhost:${port}/internal/storage/ping" 2>/dev/null | grep -q '200'; then
                ok=$((ok + 1))
            fi
        done
        if [ "$ok" -eq 3 ]; then
            started=1
            break
        fi
        sleep 5
    done

    echo ""
    echo "=============================================="
    if [ "$started" -eq 1 ]; then
        echo -e "  ${GREEN}ww 后端 3 节点已就绪${NC}"
    else
        echo -e "  ${YELLOW}ww 后端部分节点可能未就绪${NC}"
    fi
    echo "=============================================="
    echo ""
    echo "  后端节点直连（调试用）:"
    echo "    Node1 (主):  HTTP http://localhost:8081  | Cache TCP localhost:8701"
    echo "    Node2 (从):  HTTP http://localhost:8082  | Cache TCP localhost:8702"
    echo "    Node3 (从):  HTTP http://localhost:8083  | Cache TCP localhost:8703"
    echo ""
    echo "  前端需手工部署（pnpm build + nginx）"
    echo ""
    echo "  常用命令:"
    echo "    查看日志:   ./deploy.sh logs"
    echo "    查看状态:   ./deploy.sh status"
    echo "    停止服务:   ./deploy.sh down"
    echo ""
}

down() {
    log "停止并移除 ww 后端服务 ..."
    docker compose -f "${COMPOSE_FILE}" down
    log "服务已停止 ✓"
}

restart() {
    log "重启 ww 后端服务 ..."
    docker compose -f "${COMPOSE_FILE}" restart
    log "服务已重启 ✓"
}

logs() {
    docker compose -f "${COMPOSE_FILE}" logs -f --tail=100
}

status() {
    echo "=============================================="
    echo "  ww 后端集群状态"
    echo "=============================================="
    echo ""

    echo "--- 容器状态 ---"
    docker compose -f "${COMPOSE_FILE}" ps
    echo ""

    echo "--- 后端节点 HTTP 检查 ---"
    for port in 8081 8082 8083; do
        local code
        code=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:${port}/internal/storage/ping" 2>/dev/null || echo "000")
        if [ "$code" = "200" ]; then
            echo -e "  Node on :${port}  ${GREEN}健康 (HTTP ${code})${NC}"
        else
            echo -e "  Node on :${port}  ${RED}不可达 (HTTP ${code})${NC}"
        fi
    done
    echo ""

    echo "--- Cache TCP 端口检查 ---"
    for port in 8701 8702 8703; do
        if timeout 2 bash -c "echo >/dev/tcp/localhost/${port}" 2>/dev/null; then
            echo -e "  Cache TCP :${port}  ${GREEN}开放${NC}"
        else
            echo -e "  Cache TCP :${port}  ${RED}未开放${NC}"
        fi
    done
    echo ""
}

all() {
    local start_ts end_ts
    start_ts=$(date +%s)
    check_prereqs
    build
    up --skip-build
    end_ts=$(date +%s)
    echo ""
    log "一键部署完成 ✓  总耗时 $((end_ts - start_ts)) 秒"
    echo ""
    status
}

case "${1:-all}" in
    build)          check_prereqs; build ;;
    build-backend)  check_prereqs; build_backend ;;
    up)             check_prereqs; up ;;
    down)           down ;;
    restart)        restart ;;
    logs)           logs ;;
    status)         status ;;
    all)            all ;;
    *)
        echo "用法: $0 {all|build|build-backend|up|down|restart|logs|status}"
        echo ""
        echo "  all            一键编译 + 构建 + 启动（默认，无参数时执行）"
        echo "  build          编译后端 + 构建后端镜像"
        echo "  build-backend  同 build（保留兼容）"
        echo "  up             启动 3 个后端节点（自动编译 + 构建镜像）"
        echo "  down           停止并移除全部服务"
        echo "  restart        重启全部服务"
        echo "  logs           查看所有服务日志"
        echo "  status         查看服务状态"
        exit 1
        ;;
esac
