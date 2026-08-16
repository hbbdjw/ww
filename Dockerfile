# ============================================================
#  运行时镜像（单阶段）
#  jar 包由本地 Maven 编译后通过 docker build 上下文传入
#  构建流程：mvn package -DskipTests → docker build
# ============================================================

ARG RUNTIME_IMAGE=swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/khipu/openjdk21-alpine:latest

FROM ${RUNTIME_IMAGE}
WORKDIR /app

# 预创建数据目录和外部配置目录
RUN mkdir -p /app/data/storage /app/config
VOLUME ["/app/data/storage"]

# 复制本地编译好的 fat jar（由 deploy.sh 执行 mvn package 生成）
COPY admin/target/admin-1.0-SNAPSHOT.jar app.jar

# 复制外部配置（优先级高于 jar 内配置，可覆盖数据库地址等）
COPY admin/src/main/resources/application-docker.yml /app/config/application-docker.yml

# HTTP 端口（8081-8083）+ Cache TCP 端口（8701-8703）
EXPOSE 8081-8083
EXPOSE 8701-8703

# Spring Boot 自动加载 ./config/ 下的配置文件，优先级高于 jar 内部
# -Dfile.encoding=UTF-8 确保 JVM 默认使用 UTF-8 编码（HTTP 头、文件系统等）
# -Duser.timezone=Asia/Shanghai 固定 JVM 时区，避免容器默认 UTC 导致日志内嵌时间（如 p6spy %(currentTime)）偏差 8 小时
ENV LANG=C.UTF-8
ENV TZ=Asia/Shanghai
ENTRYPOINT ["java", "-Dfile.encoding=UTF-8", "-Duser.timezone=Asia/Shanghai", "-jar", "app.jar"]
