#!/usr/bin/env bash
# ============================================================
# distributed-runtime 4 节点集群一键部署脚本
# （编译 runtime-server jar + 构建 Docker 镜像 + 启动 4 节点）
#
# 用法（在 WSL 中、项目根目录执行）:
#   cd /mnt/f/ww/ww
#   ./runtime/deploy-runtime.sh            无参数 = all（一键编译 + 构建 + 启动）
#   ./runtime/deploy-runtime.sh all        一键编译 + 构建镜像 + 启动集群
#   ./runtime/deploy-runtime.sh build      编译 jar + 构建镜像
#   ./runtime/deploy-runtime.sh up         启动 4 个节点（自动编译 + 构建镜像）
#   ./runtime/deploy-runtime.sh down       停止并移除全部服务
#   ./runtime/deploy-runtime.sh restart    重启全部服务
#   ./runtime/deploy-runtime.sh logs       查看所有服务日志
#   ./runtime/deploy-runtime.sh status     查看集群状态
#
# 说明:
#   - 数据库复用 WSL 宿主机的 PostgreSQL 18（5432/ww），不新建 PG 容器。
#     要求 PG listen_addresses 非仅 localhost 且 pg_hba.conf 放行 docker 网段。
#   - WSL 内无 Maven 时自动回退调用 Windows 侧 mvnw.cmd 编译；
#     若两者都不可用则复用已有 jar（会显著警告，避免静默部署旧代码）。
#   - 故障注入：docker kill ww-rt-nodeX（restart: "no" 不会自动拉起）。
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

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$REPO_ROOT"

IMAGE_NAME="ww-runtime:latest"
DOCKERFILE="runtime/Dockerfile"
BUILD_CONTEXT="runtime"
COMPOSE_FILE="runtime/docker-compose.yml"
COMPOSE="docker compose -p ww-runtime -f ${COMPOSE_FILE}"
JAR_FILE="runtime/server/target/runtime-server-1.0-SNAPSHOT.jar"
HTTP_PORTS="8091 8092 8093 8094"
NODE_COUNT=4

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

# -----------------------------------------------------------
# 前置检查
# -----------------------------------------------------------
check_prereqs() {
    if ! command -v docker &>/dev/null; then
        err "docker 未安装（本机 Docker 位于 WSL，请在 WSL 中执行本脚本）"
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
    # 宿主机 PostgreSQL 18 连通性预检（容器经 host.docker.internal 访问同一地址）
    if timeout 2 bash -c 'echo > /dev/tcp/127.0.0.1/5432' 2>/dev/null; then
        log "宿主机 PostgreSQL 端口 5432 可达 ✓"
    else
        warn "宿主机 127.0.0.1:5432 不可达：请确认 PostgreSQL 18 已启动"
    fi
}

# -----------------------------------------------------------
# Maven 探测（与 deploy.sh 一致）+ Windows mvnw.cmd 回退
# -----------------------------------------------------------
# 项目编译要求 Java 21（pom.xml maven.compiler.release=21）
REQUIRED_JAVA_MAJOR=21

# Windows 侧常见 JDK 21 安装目录（cmd.exe 回退编译时用于设置 JAVA_HOME）
_find_win_jdk21() {
    local d
    for d in /mnt/d/jdks/corretto-21* /mnt/d/jdks/dragonwell-21* \
             /mnt/c/Program\ Files/Java/jdk-21* /mnt/c/Program\ Files/Amazon\ Corretto/jdk21*; do
        # 注意：root 下 drvfs 默认 644 权限，-x 对 .exe 恒为 false，改用 -f 判断存在性
        if [ -f "$d/bin/java.exe" ]; then
            # 转为 Windows 路径（cmd.exe 需要 D:\\... 格式）；wslpath 不可用时手工转换
            local win_path
            win_path=$(wslpath -w "$d" 2>/dev/null || true)
            if [ -z "$win_path" ]; then
                win_path=$(echo "$d" | sed -E 's|^/mnt/([a-z])|\U\1:|; s|/|\\|g')
            fi
            echo "$win_path"
            return 0
        fi
    done
    return 1
}

_mvn_works() {
    local cmd="$1"
    "$cmd" -v >/dev/null 2>&1
}

# 校验指定 java 可执行文件的主版本号 >= REQUIRED_JAVA_MAJOR
_java_major_ok() {
    local java_bin="$1"
    local ver
    ver=$("$java_bin" -version 2>&1 | head -1 | sed -E 's/.*version "([0-9]+).*/\1/')
    [ -n "$ver" ] && [ "$ver" -ge "$REQUIRED_JAVA_MAJOR" ] 2>/dev/null
}

# 探测本机常见 JDK 21 安装目录（apt/Corretto/手工解压），返回 JAVA_HOME 候选
_find_local_jdk21() {
    local d
    for d in /usr/lib/jvm/java-21* /opt/amazon-corretto-21* /opt/*jdk-21* \
             /opt/jdk21* /opt/corretto-21* "$HOME"/jdk/*21*; do
        if [ -x "$d/bin/java" ] && _java_major_ok "$d/bin/java"; then
            echo "$d"
            return 0
        fi
    done
    return 1
}

# 探测常见解压安装位置的 mvn（二进制解压安装通常不在 PATH 中）
_find_mvn_in_common_dirs() {
    local d
    for d in /opt /usr/local /usr/share "$HOME"; do
        local bin
        for bin in "$d"/apache-maven-*/bin/mvn "$d"/maven/bin/mvn; do
            if [ -x "$bin" ] && _mvn_works "$bin"; then
                echo "$bin"
                return 0
            fi
        done
    done
    return 1
}

# PATH 上的 mvn 可能是 WSL 继承 Windows PATH 带进来的 Windows 版 mvn（sh 脚本），
# root 下 drvfs 无执行位会 Permission denied，必须实际可运行才采纳
_find_mvn_on_path() {
    local bin
    bin=$(command -v mvn 2>/dev/null || true)
    if [ -n "$bin" ] && _mvn_works "$bin"; then
        echo "$bin"
        return 0
    fi
    return 1
}

check_build_environment() {
    IS_WSL=0
    if grep -qi microsoft /proc/version 2>/dev/null; then
        IS_WSL=1
    fi

    MVN_CMD=""
    # 优先原生 mvn（WSL/Linux 内安装的 Maven），但要求配套 JDK >= 21
    if MVN_CMD=$(_find_mvn_on_path); then
        :
    elif [ -n "${MAVEN_HOME:-}" ] && [ -x "$MAVEN_HOME/bin/mvn" ] && _mvn_works "$MAVEN_HOME/bin/mvn"; then
        MVN_CMD="$MAVEN_HOME/bin/mvn"
        log "使用 MAVEN_HOME: $MVN_CMD"
    elif MVN_CMD=$(_find_mvn_in_common_dirs); then
        log "使用常见安装目录的 Maven: $MVN_CMD"
    elif [ "$IS_WSL" -eq 1 ]; then
        if [ -x "/mnt/d/maven/apache-maven-3.9.0/bin/mvn.cmd" ] && _mvn_works "/mnt/d/maven/apache-maven-3.9.0/bin/mvn.cmd"; then
            MVN_CMD="/mnt/d/maven/apache-maven-3.9.0/bin/mvn.cmd"
            log "WSL2: 使用 D 盘 Maven: $MVN_CMD"
        elif command -v mvn.cmd &>/dev/null && _mvn_works "mvn.cmd"; then
            MVN_CMD="mvn.cmd"
            log "WSL2: 使用 Windows Maven (PATH): $MVN_CMD"
        fi
    fi

    # 原生 mvn 配套 JDK 版本校验：本机 JAVA_HOME/PATH 若指向 JDK < 21，
    # 先尝试探测常见安装目录的 JDK 21 并修正 JAVA_HOME；仍找不到才放弃原生 mvn
    if [ -n "$MVN_CMD" ] && [ "${MVN_CMD##*/}" != "mvn.cmd" ]; then
        local sys_java
        sys_java="$(command -v java || true)"
        if [ -n "${JAVA_HOME:-}" ] && [ -x "$JAVA_HOME/bin/java" ]; then
            sys_java="$JAVA_HOME/bin/java"
        fi
        if [ -z "$sys_java" ] || ! _java_major_ok "$sys_java"; then
            local local_jdk21
            local_jdk21=$(_find_local_jdk21 || true)
            if [ -n "$local_jdk21" ]; then
                export JAVA_HOME="$local_jdk21"
                export PATH="$JAVA_HOME/bin:$PATH"
                log "系统 java 不可用/低于 ${REQUIRED_JAVA_MAJOR}，已自动切换 JAVA_HOME=$JAVA_HOME"
            else
                warn "原生 Maven 已找到，但配套 JDK 低于 ${REQUIRED_JAVA_MAJOR}（或不可用），无法编译本项目"
                warn "放弃原生 Maven，改走 Windows 侧 JDK 21 编译回退路径"
                MVN_CMD=""
            fi
        fi
    fi

    if [ -n "$MVN_CMD" ]; then
        local mvn_v_out
        mvn_v_out=$($MVN_CMD -v 2>&1 | head -3)
        log "$(echo "$mvn_v_out" | head -1)"
    fi
}

ensure_runtime_image() {
    if docker image inspect "$RUNTIME_BASE_IMAGE" &>/dev/null; then
        log "运行时基础镜像已存在: $RUNTIME_BASE_IMAGE"
    else
        warn "运行时基础镜像不存在，正在拉取: $RUNTIME_BASE_IMAGE"
        if ! docker pull "$RUNTIME_BASE_IMAGE"; then
            err "拉取失败: $RUNTIME_BASE_IMAGE"
            exit 1
        fi
        log "拉取完成: $RUNTIME_BASE_IMAGE"
    fi
}

# -----------------------------------------------------------
# 编译 runtime-server jar
# -----------------------------------------------------------
# 编译失败/无编译能力时的兜底：复用已有 jar（显著警告）或报错退出
_fallback_or_exit() {
    if [ -f "$JAR_FILE" ]; then
        warn "!!! 将复用已有 jar（可能不是最新代码），时间: $(date -r "$JAR_FILE" '+%Y-%m-%d %H:%M:%S')"
    else
        err "jar 包不存在，无法继续"
        err "建议先在 Windows 侧（JDK 21）编译: .\\mvnw.cmd -f runtime -pl server -am -DskipTests -B"
        exit 1
    fi
}

compile_jar() {
    log "开始编译 runtime-server ..."
    if [ ! "$IS_WSL" -eq 1 ] && [ -f .mvn/jvm.config ]; then
        # jvm.config 中 stdout.encoding=GBK 是为 Windows cp936 控制台准备的，
        # 纯 Linux 下无效，通过 MAVEN_ARGS 覆盖为 UTF-8 避免乱码
        export MAVEN_ARGS="${MAVEN_ARGS:-} -Dstdout.encoding=UTF-8 -Dstderr.encoding=UTF-8"
    fi

    if [ -n "$MVN_CMD" ]; then
        $MVN_CMD -f runtime -pl server -am -DskipTests -B package
    elif [ "$IS_WSL" -eq 1 ] && command -v cmd.exe &>/dev/null; then
        # WSL 内无可用 Maven（或未配套 JDK 21）：回退调用 Windows 侧 mvnw.cmd 编译。
        # 显式指定 Windows JDK 21，避免 cmd.exe 默认 PATH 上的旧 JDK 导致编译失败。
        # 注意：root 用户下 WSL 互操作可能被禁用（Permission denied），建议用普通用户执行。
        local win_jdk21
        win_jdk21=$(_find_win_jdk21 || true)
        if [ -n "$win_jdk21" ]; then
            warn "WSL 内无可用 Maven/JDK21，回退 Windows 编译（JAVA_HOME=$win_jdk21）..."
            if ! cmd.exe /c "set JAVA_HOME=$win_jdk21&& mvnw.cmd -f runtime -pl server -am -DskipTests -B"; then
                err "Windows mvnw.cmd 编译失败"
                _fallback_or_exit
            fi
        else
            err "WSL 内无可用 Maven，且未在 Windows 常见目录找到 JDK 21"
            _fallback_or_exit
        fi
    else
        err "当前环境无法执行 Maven"
        exit 1
    fi

    if [ ! -f "$JAR_FILE" ]; then
        err "编译完成但未找到 jar: $JAR_FILE"
        exit 1
    fi
    local jar_size
    jar_size=$(du -h "$JAR_FILE" | awk '{print $1}')
    log "编译完成 ✓  jar: $JAR_FILE ($jar_size)"
}

# -----------------------------------------------------------
# 构建：编译 jar（不可用时复用旧 jar 并显著警告）+ Docker 镜像
# -----------------------------------------------------------
build_jar() {
    check_build_environment
    if [ -n "$MVN_CMD" ] || { [ "$IS_WSL" -eq 1 ] && command -v cmd.exe &>/dev/null; }; then
        compile_jar
    elif [ -f "$JAR_FILE" ]; then
        warn "!!! 未找到 Maven，将复用已有 jar（可能不是最新代码）!!!"
        warn "!!! jar 时间: $(date -r "$JAR_FILE" '+%Y-%m-%d %H:%M:%S')"
        warn "!!! 如需确保最新代码，请先在 Windows 执行:"
        warn "!!!   .\\mvnw.cmd -f runtime -pl server -am -DskipTests -B"
    else
        err "jar 包不存在，且当前环境无法执行 Maven"
        err "请先在 Windows 执行: .\\mvnw.cmd -f runtime -pl server -am -DskipTests -B"
        exit 1
    fi
}

build_image() {
    log "开始构建 runtime Docker 镜像 ${IMAGE_NAME} ..."
    if [ ! -f "$JAR_FILE" ]; then
        err "jar 包不存在: $JAR_FILE"
        err "请先执行: ./runtime/deploy-runtime.sh build"
        exit 1
    fi
    docker build --pull=false -t "${IMAGE_NAME}" -f "${DOCKERFILE}" "${BUILD_CONTEXT}"
    # 清理旧版本产生的悬空镜像层，避免磁盘占用持续增长
    docker image prune -f >/dev/null 2>&1 || true
    log "runtime 镜像构建完成 ✓"
}

build() {
    ensure_runtime_image
    build_jar
    build_image
}

# -----------------------------------------------------------
# 启动 4 节点 + 健康检查
# -----------------------------------------------------------
_node_status_ok() {
    local port="$1"
    curl -s -o /dev/null -w "%{http_code}" "http://localhost:${port}/rt/status" 2>/dev/null | grep -q '200'
}

_active_node_count() {
    # /rt/nodes 返回 JSON 数组，统计 state=ACTIVE 的节点数
    curl -s "http://localhost:${1}/rt/nodes" 2>/dev/null \
        | grep -o '"state":"ACTIVE"' | wc -l
}

up() {
    if [ "${1:-}" != "--skip-build" ]; then
        log "构建最新 jar 包与 Docker 镜像..."
        build
    fi

    log "启动 runtime 4 节点集群（强制重建容器以使用最新镜像）..."
    $COMPOSE up -d --force-recreate

    echo ""
    log "等待 4 个节点 /rt/status 就绪（最多 120 秒）..."
    local started=0
    for _ in $(seq 1 24); do
        local ok=0
        for port in $HTTP_PORTS; do
            if _node_status_ok "$port"; then
                ok=$((ok + 1))
            fi
        done
        if [ "$ok" -eq "$NODE_COUNT" ]; then
            started=1
            break
        fi
        sleep 5
    done

    local converged=0
    if [ "$started" -eq 1 ]; then
        log "等待成员关系收敛：每个节点看到 ${NODE_COUNT} 个 ACTIVE 节点（最多 60 秒）..."
        for _ in $(seq 1 30); do
            local all_ok=1
            for port in $HTTP_PORTS; do
                if [ "$(_active_node_count "$port")" -ne "$NODE_COUNT" ]; then
                    all_ok=0
                    break
                fi
            done
            if [ "$all_ok" -eq 1 ]; then
                converged=1
                break
            fi
            sleep 2
        done
    fi

    echo ""
    echo "=============================================="
    if [ "$started" -eq 1 ] && [ "$converged" -eq 1 ]; then
        echo -e "  ${GREEN}runtime 4 节点集群已就绪（成员关系已收敛）${NC}"
    elif [ "$started" -eq 1 ]; then
        echo -e "  ${YELLOW}节点已启动，但成员关系未完全收敛，请查看日志: $0 logs${NC}"
    else
        echo -e "  ${RED}部分节点未就绪，请查看日志: $0 logs${NC}"
        echo -e "  ${YELLOW}若日志为数据库连接失败，请检查 PG 的 listen_addresses / pg_hba.conf 是否放行 docker 网段${NC}"
    fi
    echo "=============================================="
    echo ""
    echo "  管理端点（REST）:"
    echo "    node-1: http://localhost:8091/rt/status | /rt/nodes"
    echo "    node-2: http://localhost:8092/rt/status | /rt/nodes"
    echo "    node-3: http://localhost:8093/rt/status | /rt/nodes"
    echo "    node-4: http://localhost:8094/rt/status | /rt/nodes"
    echo ""
    echo "  故障注入示例:"
    echo "    杀死 leader:      docker kill ww-rt-node1        (restart: no，不会自动拉起)"
    echo "    恢复节点:         docker start ww-rt-node1"
    echo "    模拟网络分区:     docker network disconnect ww-runtime-net ww-rt-node4"
    echo "    恢复网络:         docker network connect ww-runtime-net ww-rt-node4"
    echo "    下线某节点能力:   curl -X PUT http://localhost:8091/rt/nodes/node-4/capabilities/MOCK/desired"
    echo "                          -H 'Content-Type: application/json' -d '{\"desired\":\"DISABLED\"}'"
    echo ""
}

down() {
    log "停止并移除 runtime 集群 ..."
    $COMPOSE down
    log "runtime 集群已停止 ✓"
}

restart() {
    log "重启 runtime 集群 ..."
    $COMPOSE restart
    log "runtime 集群已重启 ✓"
}

logs() {
    $COMPOSE logs -f --tail=100
}

status() {
    echo "=============================================="
    echo "  runtime 4 节点集群状态"
    echo "=============================================="
    echo ""

    echo "--- 容器状态 ---"
    $COMPOSE ps
    echo ""

    echo "--- 节点健康检查 ---"
    for port in $HTTP_PORTS; do
        local code leader_flag active
        code=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:${port}/rt/status" 2>/dev/null || echo "000")
        if [ "$code" = "200" ]; then
            if curl -s "http://localhost:${port}/rt/status" 2>/dev/null | grep -q '"leader":true'; then
                leader_flag="LEADER"
            else
                leader_flag="follower"
            fi
            active=$(_active_node_count "$port")
            echo -e "  :${port}  ${GREEN}健康${NC}  role=${leader_flag}  ACTIVE 节点数=${active}/${NODE_COUNT}"
        else
            echo -e "  :${port}  ${RED}不可达 (HTTP ${code})${NC}"
        fi
    done
    echo ""
}

all() {
    local start_ts end_ts
    start_ts=$(date +%s)
    check_prereqs
    up
    end_ts=$(date +%s)
    echo ""
    log "一键部署完成 ✓  总耗时 $((end_ts - start_ts)) 秒"
    echo ""
    status
}

case "${1:-all}" in
    build)   check_prereqs; build ;;
    up)      check_prereqs; up ;;
    down)    down ;;
    restart) restart ;;
    logs)    logs ;;
    status)  status ;;
    all)     all ;;
    *)
        echo "用法: $0 {all|build|up|down|restart|logs|status}"
        echo ""
        echo "  all      一键编译 + 构建 + 启动（默认，无参数时执行）"
        echo "  build    编译 runtime-server jar + 构建镜像"
        echo "  up       启动 4 个节点（自动编译 + 构建镜像）"
        echo "  down     停止并移除全部服务"
        echo "  restart  重启全部服务"
        echo "  logs     查看所有服务日志"
        echo "  status   查看集群状态"
        exit 1
        ;;
esac
