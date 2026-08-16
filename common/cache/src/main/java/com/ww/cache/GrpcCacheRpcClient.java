package com.ww.cache;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import com.ww.rpc.core.RequestType;
import com.ww.rpc.core.RpcRequest;
import com.ww.rpc.core.RpcResponse;
import com.ww.rpc.core.RpcService;
import com.ww.rpc.handler.RpcRequestHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于 cluster-rpc（gRPC/HTTP2）的缓存节点间 RPC 客户端实现。
 *
 * <p>与 {@link CacheClient}（自定义 TCP 二进制协议）形成双实现，业务层通过
 * {@link CacheRpcClient} 接口编程，可在 TCP 与 gRPC 之间切换。
 *
 * <h3>协议映射</h3>
 * <ul>
 *   <li>所有命令走统一服务 {@value #SVC_COMMAND}，通过 header 传递 flags/opCode；
 *       payload 即为原始 argsPayload 字节。</li>
 *   <li>响应状态通过 RpcResponse.status + header {@value #H_RESP_STATUS} 双层表达：
 *       RpcResponse 本身的 status 表达 RPC 层成功/失败，业务层 STATUS_OK/ERROR/NULL
 *       通过 header 区分，payload 为原始响应字节。</li>
 *   <li>FLAG_REPLICA 场景（副本复制）使用 {@value #SVC_REPLICA} 服务，handler 立即
 *       返回空 ACK，客户端 sendAsync 以非阻塞方式提交到线程池执行。</li>
 * </ul>
 *
 * <h3>本地 handler 注册</h3>
 * <p>调用方应通过 {@link #registerLocalHandler(BiFunction)} 注册命令处理器，
 * 处理器接收 (flags, opCode) → responsePayload（或抛异常表示 STATUS_ERROR）。
 * 处理器内部可直接复用 {@link CacheServer} 已有的命令解析逻辑（如
 * {@code CacheProtocol.executeCommand(cache, opCode, payload)}）。
 */
public class GrpcCacheRpcClient implements CacheRpcClient {

    private static final Logger log = LoggerFactory.getLogger(GrpcCacheRpcClient.class);

    public static final String SVC_COMMAND = "cache.command";
    public static final String SVC_REPLICA = "cache.replica";

    /** header: opCode (int string, 0-255) */
    static final String H_OP = "x-cache-op";
    /** header: flags (int string) */
    static final String H_FLAGS = "x-cache-flags";
    /** header: auth token */
    static final String H_AUTH = "x-cache-auth";
    /**
     * header: response status — "ok" / "null" / "error"
     * （仅 RPC 层成功时有效；RPC 层失败通过 RpcResponse.status 表达）
     */
    static final String H_RESP_STATUS = "x-cache-status";
    static final String RESP_OK = "ok";
    static final String RESP_NULL = "null";
    static final String RESP_ERROR = "error";

    private static final long DEFAULT_TIMEOUT_MS = 10_000L;
    private static final long ASYNC_TIMEOUT_MS = 5_000L;

    private final String host;
    private final int port;
    private final String authToken;
    private final RpcService rpcService;
    private final long defaultTimeoutMs;

    /** fire-and-forget 用的小线程池；与 TCP sendAsync 语义对齐：不阻塞调用方线程 */
    private final ExecutorService asyncExecutor;

    public GrpcCacheRpcClient(String host, int port, String authToken, RpcService rpcService) {
        this(host, port, authToken, rpcService, DEFAULT_TIMEOUT_MS);
    }

    public GrpcCacheRpcClient(String host, int port, String authToken,
                              RpcService rpcService, long defaultTimeoutMs) {
        this.host = host;
        this.port = port;
        this.authToken = authToken;
        this.rpcService = rpcService;
        this.defaultTimeoutMs = defaultTimeoutMs;
        this.asyncExecutor = newAsyncExecutor();
    }

    private static ExecutorService newAsyncExecutor() {
        // 限制并发数，避免大批量副本复制撑爆线程；守护线程，不阻止 JVM 退出
        ThreadPoolExecutor pool = new ThreadPoolExecutor(
                2, 8, 60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(10_000),
                r -> {
                    Thread th = new Thread(r, "cache-rpc-replica");
                    th.setDaemon(true);
                    return th;
                },
                new ThreadPoolExecutor.CallerRunsPolicy() // 队列满时调用方线程协助执行，提供背压
        );
        pool.allowCoreThreadTimeOut(true);
        return pool;
    }

    /**
     * 在本地 RpcService 上注册 cache 命令处理器。
     *
     * @param handler  (flags, opCode, payload) → responsePayload；抛异常等价于 STATUS_ERROR
     */
    public void registerLocalHandler(CommandHandler handler) {
        RpcRequestHandler cmdHandler = (req, ctx) -> handleCommand(req, handler, false);
        RpcRequestHandler replicaHandler = (req, ctx) -> handleCommand(req, handler, true);
        rpcService.registerHandler(SVC_COMMAND, cmdHandler);
        rpcService.registerHandler(SVC_REPLICA, replicaHandler);
    }

    private RpcResponse handleCommand(RpcRequest req, CommandHandler handler, boolean replica) {
        String reqId = req.getRequestId();
        byte flags = (byte) parseIntHeader(req.getHeaders(), H_FLAGS, 0);
        byte opCode = (byte) parseIntHeader(req.getHeaders(), H_OP, 0);
        byte[] payload = req.getPayload();
        try {
            byte[] respBytes = handler.handle(flags, opCode, payload);
            Map<String, String> respHeaders = new HashMap<>();
            String status;
            byte[] body;
            if (respBytes == null) {
                status = RESP_NULL;
                body = new byte[0];
            } else {
                status = RESP_OK;
                body = respBytes;
            }
            return RpcResponse.newBuilder()
                    .requestId(reqId)
                    .status(RpcResponse.Status.OK)
                    .payload(body)
                    .headers(respHeaders)
                    .header(H_RESP_STATUS, status)
                    .build();
        } catch (Exception e) {
            String errMsg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
            log.debug("cache command handler error op=0x{}: {}", Integer.toHexString(opCode & 0xFF), errMsg);
            byte[] errPayload = Codec.encodeValue(errMsg);
            return RpcResponse.newBuilder()
                    .requestId(reqId)
                    .status(RpcResponse.Status.OK)
                    .payload(errPayload)
                    .header(H_RESP_STATUS, RESP_ERROR)
                    .build();
        }
    }

    private static int parseIntHeader(Map<String, String> headers, String key, int dflt) {
        String v = headers.get(key);
        if (v == null || v.isEmpty()) return dflt;
        try {
            return Integer.parseInt(v);
        } catch (NumberFormatException e) {
            return dflt;
        }
    }

    @Override
    public byte[] send(byte flags, byte opCode, byte[] argsPayload, String purpose) throws IOException {
        String address = address();
        String svc = (flags & CacheProtocol.FLAG_REPLICA) != 0 ? SVC_REPLICA : SVC_COMMAND;
        RequestType type = (flags & CacheProtocol.FLAG_REPLICA) != 0 ? RequestType.REPLICATION : RequestType.WRITE;
        // 读类 op 标记为 READ，让 RPC 层可做只读路由/metrics
        if (isReadOp(opCode)) {
            type = RequestType.READ;
        } else if (isClusterOp(opCode)) {
            type = RequestType.CLUSTER_ADMIN;
        } else if (opCode == (byte) 0x80 /* PING */) {
            type = RequestType.HEARTBEAT;
        }

        Map<String, String> headers = new HashMap<>();
        headers.put(H_OP, String.valueOf(opCode & 0xFF));
        headers.put(H_FLAGS, String.valueOf(flags & 0xFF));
        if (authToken != null && !authToken.isEmpty()) {
            headers.put(H_AUTH, authToken);
        }
        RpcRequest rpcReq = RpcRequest.newBuilder()
                .requestId(UUID.randomUUID().toString())
                .serviceName(svc)
                .type(type)
                .payload(argsPayload != null ? argsPayload : new byte[0])
                .headers(headers)
                .build();
        RpcResponse resp = rpcService.send(address, rpcReq, defaultTimeoutMs);
        if (!resp.isSuccess()) {
            throw new IOException("cache RPC " + purpose + " failed to " + address
                    + ": " + resp.getStatus() + " " + resp.getErrorMessage());
        }
        String cacheStatus = resp.getHeader(H_RESP_STATUS);
        if (cacheStatus == null) cacheStatus = RESP_OK;
        if (RESP_ERROR.equals(cacheStatus)) {
            String errMsg = "remote error";
            byte[] pl = resp.getPayload();
            if (pl != null && pl.length > 0) {
                Object decoded = Codec.decodeValue(pl);
                if (decoded instanceof String s) errMsg = s;
            }
            throw new RemoteCacheException(errMsg);
        }
        if (RESP_NULL.equals(cacheStatus)) {
            return new byte[0];
        }
        return resp.getPayload();
    }

    @Override
    public void sendAsync(byte flags, byte opCode, byte[] argsPayload, String purpose) {
        // 副本复制走异步线程池，不阻塞调用方（与 TCP sendAsync fire-and-forget 语义对齐）
        byte[] payload = argsPayload != null ? argsPayload.clone() : new byte[0];
        asyncExecutor.execute(() -> {
            try {
                send(flags, opCode, payload, purpose + "[async]");
            } catch (Exception e) {
                // 与 TCP 实现一致：复制失败不抛回调用方（由上层健康检查/backlog 机制补偿）
                log.debug("cache async send failed to {}:{} op=0x{}: {}", host, port,
                        Integer.toHexString(opCode & 0xFF), e.getMessage());
            }
        });
    }

    @Override
    public String getHost() {
        return host;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public void close() {
        asyncExecutor.shutdown();
        try {
            if (!asyncExecutor.awaitTermination(2, TimeUnit.SECONDS)) {
                asyncExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            asyncExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private String address() {
        return host + ":" + port;
    }

    private static boolean isReadOp(byte opCode) {
        return switch (opCode) {
            case CacheProtocol.OP_GET,
                 CacheProtocol.OP_SIZE, CacheProtocol.OP_KEYS,
                 CacheProtocol.OP_HGET, CacheProtocol.OP_HMGET, CacheProtocol.OP_HGETALL,
                 CacheProtocol.OP_LPOP, CacheProtocol.OP_RPOP,
                 CacheProtocol.OP_SMEMBERS,
                 CacheProtocol.OP_ZRANGE, CacheProtocol.OP_ZRANGEBYSCORE,
                 CacheProtocol.OP_XLEN, CacheProtocol.OP_XRANGE, CacheProtocol.OP_XREVRANGE,
                 CacheProtocol.OP_XREAD, CacheProtocol.OP_XREADGROUP,
                 CacheProtocol.OP_XINFO, CacheProtocol.OP_XINFO_GROUPS, CacheProtocol.OP_XINFO_CONSUMERS,
                 CacheProtocol.OP_XPENDING,
                 CacheProtocol.OP_CLUSTER_STATUS -> true;
            default -> false;
        };
    }

    private static boolean isClusterOp(byte opCode) {
        return switch (opCode) {
            case CacheProtocol.OP_PING, CacheProtocol.OP_PONG,
                 CacheProtocol.OP_NODE_JOIN, CacheProtocol.OP_NODE_LEAVE,
                 CacheProtocol.OP_PROMOTE,
                 CacheProtocol.OP_MIGRATE_PULL, CacheProtocol.OP_MIGRATE_PUSH,
                 CacheProtocol.OP_GOSSIP -> true;
            default -> false;
        };
    }

    /**
     * 本地命令处理器函数式接口：接收 flags + opCode + payload，返回响应 payload。
     * 返回 null 表示 STATUS_NULL；抛出异常表示 STATUS_ERROR。
     */
    @FunctionalInterface
    public interface CommandHandler {
        byte[] handle(byte flags, byte opCode, byte[] payload) throws Exception;
    }
}
