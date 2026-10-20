#!/usr/bin/env bash

if [ -z "${BASH_VERSION:-}" ]; then
    if command -v bash >/dev/null 2>&1; then
        exec bash "$0" "$@"
    else
        echo "ERR: 需要 bash，但当前环境未找到 bash" >&2
        exit 1
    fi
fi

set -euo pipefail

# 任何命令失败导致 set -e 退出时，打印失败行号与命令本身，
# 避免脚本静默退出难以定位（如前端构建后无声消失的问题）。
trap 'err "脚本异常退出：第 ${LINENO} 行命令失败 → ${BASH_COMMAND}"' ERR

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

# 管理面凭据：开启 Spring Security 后 /rt/status、/rt/nodes 等需 Bearer token。
# 默认值与 RbacSeeder 首次播种的账号一致（仅零用户时播种，redeploy 不会覆盖已改密码）。
# 若已在 UI 中修改过 admin 密码，请用环境变量覆盖：
#   RUNTIME_ADMIN_PASSWORD=新密码 ./runtime/deploy-runtime.sh status
ADMIN_USER="${RUNTIME_ADMIN_USER:-admin}"
ADMIN_PASSWORD="${RUNTIME_ADMIN_PASSWORD:-admin123}"

RUNTIME_BASE_IMAGE="swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/khipu/openjdk21-alpine:latest"

# 数据库连接（与 docker-compose.yml x-runtime-common 保持一致；
# 宿主机视角用 127.0.0.1，容器内经 host.docker.internal 访问同一实例）
PG_HOST="127.0.0.1"
PG_PORT="5432"
PG_DB="ww"
PG_USER="postgres"
PG_PASSWORD="jw3344..."

MVN_CMD=""
PNPM_CMD=""
IS_WSL=0
if grep -qi microsoft /proc/version 2>/dev/null; then
    IS_WSL=1
fi

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
HAS_WSL_INTEROP=0

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
    # 检测 WSL 互操作性（root 用户默认可能被禁用）
    if [ "$IS_WSL" -eq 1 ] && command -v cmd.exe &>/dev/null; then
        if cmd.exe /c "echo ok" >/dev/null 2>&1; then
            HAS_WSL_INTEROP=1
        fi
    fi
    # root 用户警告：WSL 互操作可能被禁用导致无法回退 Windows 编译
    if [ "$(id -u)" -eq 0 ]; then
        warn "⚠️  当前以 root 用户执行："
        warn "    - WSL 互操作性(cmd.exe)可能被禁用，Maven 编译失败时无法自动回退 Windows 侧构建"
        if [ "$HAS_WSL_INTEROP" -eq 0 ] && [ "$IS_WSL" -eq 1 ]; then
            warn "    - 检测到 cmd.exe 不可用，若 protobuf 编译失败请先在 Windows PowerShell 执行："
            warn "      cd F:\\ww\\ww"
            warn "      .\\mvnw.cmd -f runtime -pl server -am -DskipTests -B clean package"
        fi
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
        # mvn -v 实际输出 5 行，head -3 截断会使 mvn 收到 SIGPIPE(141)，
        # pipefail 下曾导致脚本在前端构建完成后静默退出，必须兜底。
        mvn_v_out=$($MVN_CMD -v 2>&1 | head -3 || true)
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

# 彻底清理 proto 模块 target 目录（WSL+NTFS 文件锁问题）。
# 每轮编译（WSL mvn / Windows mvnw.cmd 回退）前都必须调用：
# 上一轮编译失败残留的半成品/锁文件会毒害下一轮（protobuf 插件
# 'Unable to clean up temporary proto file directory' / javac 读取失败）。
_clean_proto_target() {
    [ -d runtime/proto/target ] || return 0
    log "清理 proto 模块构建缓存..."
    # 先加权限再删除
    chmod -R u+w runtime/proto/target 2>/dev/null || true
    if ! rm -rf runtime/proto/target 2>/dev/null; then
        # WSL rm 失败，尝试用 Windows cmd.exe 删除（NTFS 权限问题 Windows 侧处理更可靠）
        if [ "$HAS_WSL_INTEROP" -eq 1 ]; then
            warn "WSL rm 清理失败，使用 Windows cmd.exe 删除 target 目录..."
            local win_proto_dir
            win_proto_dir=$(wslpath -w "$REPO_ROOT/runtime/proto" 2>/dev/null || echo "F:\\ww\\ww\\runtime\\proto")
            cmd.exe /c "rd /s /q \"${win_proto_dir}\\target\"" 2>/dev/null || true
        fi
    fi
    # drvfs/9p 句柄释放有延迟：删除后轮询确认，残留则显著警告
    local wait_i
    for wait_i in 1 2 3; do
        [ -d runtime/proto/target ] || return 0
        sleep 1
        rm -rf runtime/proto/target 2>/dev/null || true
    done
    if [ -d runtime/proto/target ]; then
        warn "!!! proto target 目录仍无法删除（可能被 Windows 进程/Defender 锁定）"
        warn "!!! 请关闭占用 F:\\ww\\ww 的 IDE/索引器后重试，或手动删除 runtime/proto/target"
    fi
}

compile_jar() {
    log "开始编译 runtime-server ..."
    if [ ! "$IS_WSL" -eq 1 ] && [ -f .mvn/jvm.config ]; then
        # jvm.config 中 stdout.encoding=GBK 是为 Windows cp936 控制台准备的，
        # 纯 Linux 下无效，通过 MAVEN_ARGS 覆盖为 UTF-8 避免乱码
        export MAVEN_ARGS="${MAVEN_ARGS:-} -Dstdout.encoding=UTF-8 -Dstderr.encoding=UTF-8"
    fi

    local mvn_ok=0

    # 彻底清理 proto 模块 target 目录（WSL+NTFS 文件锁问题）
    if [ "$IS_WSL" -eq 1 ]; then
        _clean_proto_target
    fi

    if [ -n "$MVN_CMD" ]; then
        MVN_GOALS="clean package"
        log "执行: $MVN_CMD -f runtime -pl server -am -DskipTests -B $MVN_GOALS"
        if $MVN_CMD -f runtime -pl server -am -DskipTests -B $MVN_GOALS; then
            mvn_ok=1
        elif [ "$IS_WSL" -eq 1 ] && [ "$HAS_WSL_INTEROP" -eq 1 ]; then
            warn "WSL 原生 Maven 编译失败（可能是 protobuf 临时目录锁定问题），自动回退 Windows 侧编译..."
            _compile_with_windows_mvn && mvn_ok=1
        fi
    elif [ "$IS_WSL" -eq 1 ] && [ "$HAS_WSL_INTEROP" -eq 1 ]; then
        _compile_with_windows_mvn && mvn_ok=1
    else
        err "当前环境无法执行 Maven"
        if [ "$IS_WSL" -eq 1 ] && [ "$(id -u)" -eq 0 ] && [ "$HAS_WSL_INTEROP" -eq 0 ]; then
            err ""
            err "root 用户下 WSL 互操作(cmd.exe)不可用，这是WSL安全限制。"
            err "解决方案："
            err "  方案1（推荐）: 退出 root 用户，使用普通用户执行脚本："
            err "    exit"
            err "    cd /mnt/f/ww/ww"
            err "    ./runtime/deploy-runtime.sh all"
            err ""
            err "  方案2: 先在 Windows PowerShell 中编译 jar："
            err "    cd F:\\ww\\ww"
            err "    .\\mvnw.cmd -f runtime -pl server -am -DskipTests -B clean package"
            err "    然后回到WSL重新执行此脚本"
        fi
        exit 1
    fi

    if [ "$mvn_ok" -ne 1 ]; then
        _fallback_or_exit
        return
    fi

    if [ ! -f "$JAR_FILE" ]; then
        err "编译完成但未找到 jar: $JAR_FILE"
        exit 1
    fi
    local jar_size
    jar_size=$(du -h "$JAR_FILE" | awk '{print $1}')
    log "编译完成 ✓  jar: $JAR_FILE ($jar_size)"
}

# WSL 环境：调用 Windows 侧 mvnw.cmd 编译
_compile_with_windows_mvn() {
    local win_jdk21
    win_jdk21=$(_find_win_jdk21 || true)
    if [ -z "$win_jdk21" ]; then
        err "WSL 内 Maven 编译失败，且未在 Windows 常见目录找到 JDK 21"
        return 1
    fi
    # 关键：清掉 WSL mvn 失败残留的 proto target（残锁/半成品会让
    # Windows 侧 javac 读取 generated-sources 失败，导致两路编译全挂）
    _clean_proto_target
    warn "使用 Windows 侧 mvnw.cmd 编译（JAVA_HOME=$win_jdk21）..."
    warn "注意：root 用户下 WSL 互操作可能被禁用，建议使用普通用户执行部署脚本"
    if cmd.exe /c "set JAVA_HOME=$win_jdk21&& mvnw.cmd -f runtime -pl server -am -DskipTests -B clean package"; then
        return 0
    else
        err "Windows mvnw.cmd 编译也失败了"
        return 1
    fi
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
# 节点探测与认证
#
# /rt/health 无需鉴权（响应体仅 UP/STARTING，供探针使用）；
# /rt/status、/rt/nodes 含集群细节，开启 Spring Security 后需 Bearer token，
# 故本脚本登录一次并缓存 token（会话存于集群 CACHE，超时或节点重启后失效 →
# 自动清缓存重登一次）。
#
# 缓存落盘而非进程内变量：调用点均在 $(...) 命令替换的子 shell 中，
# 子 shell 对变量的赋值不会回传父进程，进程内缓存会失效导致每轮重复登录
# （BCrypt 校验开销不小，收敛轮询最多会放大到上百次）。
# TOKEN_DIR 必须在此处（顶层）创建——若放进函数，子 shell 内赋值同样不回传，
# 每次调用都会新建目录而使缓存永不命中。
# -----------------------------------------------------------
TOKEN_DIR=$(mktemp -d)
trap 'rm -rf "$TOKEN_DIR"' EXIT

_node_token() {
    local port="$1" cache_file="${TOKEN_DIR}/token-${1}"
    if [ -s "$cache_file" ]; then
        cat "$cache_file"
        return 0
    fi
    local body token
    body=$(curl -s --max-time 5 -X POST "http://localhost:${port}/api/auth/login" \
        -H 'Content-Type: application/json' \
        -d "{\"username\":\"${ADMIN_USER}\",\"password\":\"${ADMIN_PASSWORD}\"}" 2>/dev/null || true)
    token=$(echo "$body" | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
    if [ -z "$token" ]; then
        return 1
    fi
    printf '%s' "$token" > "$cache_file"
    echo "$token"
    return 0
}

# 带认证 GET；token 失效时清缓存重登重试一次。失败返回非 0。
_auth_get() {
    local port="$1" path="$2"
    local token body cache_file="${TOKEN_DIR}/token-${1}"
    token=$(_node_token "$port") || return 1
    body=$(curl -s --max-time 5 -H "Authorization: Bearer ${token}" \
        "http://localhost:${port}${path}" 2>/dev/null || true)
    if [ -z "$body" ]; then
        rm -f "$cache_file"
        token=$(_node_token "$port") || return 1
        body=$(curl -s --max-time 5 -H "Authorization: Bearer ${token}" \
            "http://localhost:${port}${path}" 2>/dev/null || true)
    fi
    [ -n "$body" ] || return 1
    echo "$body"
}

# 节点角色：LEADER / follower / unknown（需认证）
# 注意：/rt/status 中布尔字段为 isLeader，leader 字段是 leader 节点名（字符串），
# 旧实现 grep '"leader":true' 永不匹配，导致角色恒显示 follower。
_node_role() {
    local port="$1" body
    body=$(_auth_get "$port" "/rt/status" 2>/dev/null || true)
    if [ -z "$body" ]; then
        echo "unknown"
    elif echo "$body" | grep -q '"isLeader":true'; then
        echo "LEADER"
    else
        echo "follower"
    fi
}

# -----------------------------------------------------------
# 启动 4 节点 + 健康检查
# -----------------------------------------------------------
_node_status_ok() {
    local port="$1"
    curl -s -o /dev/null -w "%{http_code}" "http://localhost:${port}/rt/health" 2>/dev/null | grep -q '200'
}

_active_node_count() {
    # /rt/nodes 返回 JSON 数组，统计 state=ACTIVE 的节点数（含集群细节，需认证）
    _auth_get "${1}" "/rt/nodes" 2>/dev/null | grep -o '"state":"ACTIVE"' | wc -l
}

# -----------------------------------------------------------
# 部署后静态资源校验：容器实际服务的 index.html 引用的 JS
# 必须与本地最新构建产物一致，否则说明容器跑的是旧镜像/旧 jar
# -----------------------------------------------------------
verify_static() {
    local local_bundle local_name served port mismatch=0
    local_bundle=$(ls "${REPO_ROOT}/runtime/server/src/main/resources/static/assets/"index-*.js 2>/dev/null | head -1)
    if [ -z "$local_bundle" ]; then
        warn "本地未找到前端构建产物 index-*.js，跳过静态资源校验"
        return 0
    fi
    local_name=$(basename "$local_bundle")
    for port in $HTTP_PORTS; do
        served=$(curl -s --max-time 5 "http://localhost:${port}/" 2>/dev/null \
            | grep -o 'assets/index-[^"]*\.js' | head -1)
        if [ "$served" != "assets/${local_name}" ]; then
            err "节点 :${port} 提供的前端产物为 ${served:-<无>}，与本地 ${local_name} 不一致（容器可能运行旧镜像）"
            mismatch=1
        fi
    done
    if [ "$mismatch" -ne 0 ]; then
        err "静态资源校验未通过！请重新执行: $0 build && $0 up --skip-build"
        return 1
    fi
    log "静态资源校验通过 ✓  ${NODE_COUNT} 节点均提供 ${local_name}"
}

# -----------------------------------------------------------
# 数据卷清理：仅在显式触发时执行（up --purge / RUNTIME_PURGE_VOLUMES=1 /
# purge 子命令），不再每次部署都清理。
# 清理动机：旧 storage/cache 数据会与新代码的分区/副本布局冲突，扰乱运行
# -----------------------------------------------------------
purge_volumes() {
    log "清理 runtime 数据卷（恢复初始化状态）..."
    # down -v 停止并移除容器 + 项目托管卷；--remove-orphans 兜底孤儿容器
    $COMPOSE down --remove-orphans -v --timeout 10 >/dev/null 2>&1 || true
    # 显式删除声明卷（兼容历史项目前缀/无前缀卷名，确保无残留）
    local i vol
    for i in $(seq 1 "$NODE_COUNT"); do
        for vol in "ww-storage-node${i}" "ww-cache-node${i}"; do
            docker volume rm -f "ww-runtime_${vol}" >/dev/null 2>&1 || true
            docker volume rm -f "${vol}" >/dev/null 2>&1 || true
        done
    done
    log "数据卷已清理 ✓"
}

# -----------------------------------------------------------
# 数据库清理：清空 rt_* 控制面表（节点注册/租约/placement/
# 迁移任务/日志等），旧数据残留会毒害新代码的引导与调度。
#
# rt_lb_config（负载均衡配置）默认保留：LB 配置持久化于数据库，
# 重新部署/重启后继续生效，不应每次部署都重置；
# 仅显式传 --include-lb（up --purge / purge-db / down）才一并清空。
# -----------------------------------------------------------
_psql() {
    PGPASSWORD="$PG_PASSWORD" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" \
        -v ON_ERROR_STOP=1 -tA -c "$1"
}

purge_database() {
    # 参数：best-effort（连接失败只警告不退出，用于 down 场景）
    #       --include-lb（连同 rt_lb_config 一起清空，显式全量重置时使用）
    local soft=0 include_lb=0
    local arg
    for arg in "$@"; do
        case "$arg" in
            best-effort) soft=1 ;;
            --include-lb) include_lb=1 ;;
        esac
    done
    _fail() {
        if [ "$soft" -eq 1 ]; then warn "$*"; return 1; fi
        err "$*"; exit 1
    }
    if ! command -v psql &>/dev/null; then
        _fail "未找到 psql，无法清空数据库（请安装 postgresql-client 后重试）" || return 0
    fi
    if ! _psql "SELECT 1" >/dev/null 2>&1; then
        _fail "数据库连接失败: postgresql://${PG_USER}@${PG_HOST}:${PG_PORT}/${PG_DB}，请确认 PostgreSQL 已启动且密码正确" || return 0
    fi
    log "清空 runtime 控制面表（${PG_DB}@${PG_HOST}:${PG_PORT}）..."
    local tables
    # 动态枚举全部 rt_* 表，未来新增表也自动纳入清理；
    # 默认排除 rt_lb_config——LB 配置保存于数据库，重启/重新部署后需继续生效
    local lb_filter=""
    if [ "$include_lb" -ne 1 ]; then
        lb_filter=" AND tablename <> 'rt_lb_config'"
    fi
    tables=$(_psql "SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename LIKE 'rt\_%'${lb_filter}")
    if [ -z "$tables" ]; then
        log "无 rt_* 表，数据库已是初始状态 ✓"
        return 0
    fi
    local list
    list=$(echo "$tables" | paste -sd, -)
    _psql "TRUNCATE TABLE ${list} RESTART IDENTITY CASCADE" >/dev/null
    if [ "$include_lb" -eq 1 ]; then
        log "数据库已清空 ✓  表: $(echo "$tables" | tr '\n' ' ')"
    else
        log "数据库已清空 ✓  表: $(echo "$tables" | tr '\n' ' ')"
        log "已保留 rt_lb_config（负载均衡配置，下次启动自动加载）"
    fi
}

# -----------------------------------------------------------
# 数据卷清理开关：默认关闭——部署(up/all)时保留旧 cache/storage 数据，
# 支持滚动升级/重启后继续使用已有数据；仅在显式要求时才全量清理：
#   --purge 参数        仅本次部署清理
#   RUNTIME_PURGE_VOLUMES=1  环境变量形式（等效 --purge）
#   ./deploy-runtime.sh purge  独立清理命令（不动数据库）
# 注意：清理后节点从零初始化，旧 storage/cache 数据不可恢复
# -----------------------------------------------------------
PURGE_VOLUMES_FLAG="${RUNTIME_PURGE_VOLUMES:-0}"

up() {
    local skip_build=0
    local purge_vol="${PURGE_VOLUMES_FLAG}"
    local arg
    for arg in "$@"; do
        case "$arg" in
            --skip-build) skip_build=1 ;;
            --purge)      purge_vol=1 ;;
            *) err "up 不支持参数: $arg（可用 --skip-build / --purge）"; exit 1 ;;
        esac
    done

    if [ "$skip_build" -ne 1 ]; then
        log "构建最新 jar 包与 Docker 镜像..."
        build
    fi

    if [ "$purge_vol" = "1" ]; then
        purge_volumes
        # --purge = 显式要求全新初始化：连同 LB 配置一起重置
        purge_database --include-lb
    else
        warn "跳过数据卷清理：保留旧 cache/storage 数据（如需恢复初始化状态请加 --purge 或设置 RUNTIME_PURGE_VOLUMES=1）"
        # 默认保留 rt_lb_config：负载均衡配置持久化于数据库，重启/重新部署后继续生效
        purge_database
    fi

    log "启动 runtime 4 节点集群（强制重建容器以使用最新镜像）..."
    $COMPOSE up -d --force-recreate

    echo ""
    log "等待 4 个节点 /rt/health 就绪（最多 120 秒）..."
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
    # 校验失败只红字报错不阻断：容器已就绪可用，但操作员必须看到产物不一致
    verify_static || true

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
    echo "  管理界面（UI）:"
    echo "    node-1: http://localhost:8091/     （登录账号默认 admin / admin123）"
    echo "    node-2: http://localhost:8092/"
    echo "    node-3: http://localhost:8093/"
    echo "    node-4: http://localhost:8094/"
    echo ""
    echo "  探针与 REST API:"
    echo "    存活探针（无需鉴权）: http://localhost:8091/rt/health"
    echo "    管理端点（需 Bearer token）: /rt/status | /rt/nodes"
    echo "    取 token:"
    echo "      TOKEN=\$(curl -s -X POST http://localhost:8091/api/auth/login \\"
    echo "        -H 'Content-Type: application/json' \\"
    echo "        -d '{\"username\":\"admin\",\"password\":\"admin123\"}' | sed -n 's/.*\"token\":\"\\([^\"]*\\)\".*/\\1/p')"
    echo "      curl -s -H \"Authorization: Bearer \$TOKEN\" http://localhost:8091/rt/status"
    echo ""
    echo "  故障注入示例:"
    echo "    杀死 leader:      docker kill ww-rt-node1        (restart: no，不会自动拉起)"
    echo "    恢复节点:         docker start ww-rt-node1"
    echo "    模拟网络分区:     docker network disconnect ww-runtime-net ww-rt-node4"
    echo "    恢复网络:         docker network connect ww-runtime-net ww-rt-node4"
    echo "    下线某节点能力:   curl -X PUT http://localhost:8091/rt/nodes/node-4/capabilities/MOCK/desired \\"
    echo "                          -H \"Authorization: Bearer \$TOKEN\" \\"
    echo "                          -H 'Content-Type: application/json' -d '{\"desired\":\"DISABLED\"}'"
    echo ""
}

down() {
    log "停止并移除 runtime 集群（含数据卷）..."
    $COMPOSE down --remove-orphans -v
    purge_database best-effort --include-lb
    log "runtime 集群已停止 ✓"
}

restart() {
    log "重启 runtime 集群 ..."
    $COMPOSE restart
    log "runtime 集群已重启 ✓"
}

# 独立清理命令：只清数据卷，不动数据库（数据库清理用 purge-db）
purge() {
    check_prereqs
    purge_volumes
    echo ""
    log "数据卷已清理 ✓  如需同时清空数据库请执行: $0 purge-db"
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
        code=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:${port}/rt/health" 2>/dev/null || echo "000")
        if [ "$code" != "200" ]; then
            echo -e "  :${port}  ${RED}不可达 (HTTP ${code})${NC}"
            continue
        fi
        leader_flag=$(_node_role "$port")
        if [ "$leader_flag" = "unknown" ]; then
            # 探针通过但认证/详情查询失败：多为凭据不符（admin 密码已改）或会话缓存未就绪
            echo -e "  :${port}  ${GREEN}存活${NC}  ${YELLOW}集群详情不可用（请确认管理凭据，见 RUNTIME_ADMIN_USER/PASSWORD）${NC}"
            continue
        fi
        # pipefail 下 curl 瞬时失败会让脚本静默退出，兜底为 0
        active=$(_active_node_count "$port" || echo 0)
        echo -e "  :${port}  ${GREEN}健康${NC}  role=${leader_flag}  ACTIVE 节点数=${active}/${NODE_COUNT}"
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
    up)      shift; check_prereqs; up "$@" ;;
    down)    down ;;
    restart) restart ;;
    purge)   purge ;;
    logs)    logs ;;
    status)  status ;;
    purge-db) check_prereqs; purge_database --include-lb ;;
    verify)  verify_static ;;
    all)     all ;;
    *)
        echo "用法: $0 {all|build|up|down|restart|purge|logs|status|purge-db|verify}"
        echo ""
        echo "  all      一键编译 + 构建 + 启动（默认保留数据卷与 LB 配置，见 up）"
        echo "  build    编译 runtime-server jar + 构建镜像"
        echo "  up       启动 4 个节点（自动编译 + 构建 + 清空 rt_* 控制面表）"
        echo "           数据卷与 rt_lb_config（负载均衡配置）默认保留不清空，"
        echo "           下次启动自动加载；需要全新初始化时加 --purge（连同 LB 配置一并重置），"
        echo "           或设置环境变量 RUNTIME_PURGE_VOLUMES=1"
        echo "           --skip-build  跳过编译与镜像构建"
        echo "  down     停止并移除全部服务（含数据卷 + 清空数据库含 LB 配置）"
        echo "  restart  重启全部服务（保留数据卷与数据库，LB 配置不受影响）"
        echo "  purge    显式清理全部数据卷（恢复初始化状态，不动数据库与 LB 配置）"
        echo "  purge-db 显式清空全部 rt_* 表（含 rt_lb_config，LB 配置恢复出厂默认）"
        echo "  logs     查看所有服务日志"
        echo "  status   查看集群状态"
        echo "  verify   校验各节点实际提供的前端产物与本地构建一致"
        exit 1
        ;;
esac
