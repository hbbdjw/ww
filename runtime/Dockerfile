# ============================================================
#  distributed-runtime 运行时镜像（单阶段）
#  jar 包由 Maven 编译后通过 docker build 上下文（runtime/ 目录）传入
#  构建流程：./runtime/deploy-runtime.sh all（编译 + docker build + 启动集群）
# ============================================================

ARG RUNTIME_IMAGE=swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/khipu/openjdk21-alpine:latest

FROM ${RUNTIME_IMAGE}
WORKDIR /app

# 预创建数据目录：对象存储 Drive 与缓存 AOF
RUN mkdir -p /app/data/storage/d1 /app/data/storage/d2 /app/data/storage/d3 /app/data/storage/d4 /app/data/cache
VOLUME ["/app/data/storage", "/app/data/cache"]

# 复制本地编译好的 fat jar（构建上下文为 runtime/ 目录）
COPY server/target/runtime-server-1.0-SNAPSHOT.jar app.jar

# REST 管理端口（SERVER_PORT，compose 中为 8091-8094）+ gRPC 端口（9100）
EXPOSE 8080-8100
EXPOSE 9100

# -Dfile.encoding=UTF-8 确保 JVM 默认使用 UTF-8 编码
# -Duser.timezone=Asia/Shanghai 固定 JVM 时区，避免容器默认 UTC 导致日志时间偏差 8 小时
# -Dsun.net.inetaddr.negative.ttl=0 / ttl=10 禁用 DNS 负缓存并缩短正缓存：
#   容器同名重启/后启动场景下，负缓存会导致 gRPC channel 永久 Unable to resolve host
ENV LANG=C.UTF-8
ENV TZ=Asia/Shanghai
# 显式钉住 JVM 堆：迁移 exportPartition 会把分区数据批量载入堆，
# 默认人体工学堆（容器内存 1/4）在 8 并发迁移下极易 OOM。
# HeapDumpOnOutOfMemoryError 保留现场：容器 restart: "no" 保持 exited，
# 事后 docker cp <容器>:/app/data/heapdump.hprof 取出分析。
ENV JAVA_OPTS="-Xms4g -Xmx4g -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/app/data/heapdump.hprof"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -Dfile.encoding=UTF-8 -Duser.timezone=Asia/Shanghai -Dsun.net.inetaddr.negative.ttl=0 -Dsun.net.inetaddr.ttl=10 -jar app.jar"]
