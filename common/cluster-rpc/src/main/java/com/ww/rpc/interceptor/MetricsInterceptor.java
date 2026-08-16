package com.ww.rpc.interceptor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.ww.rpc.core.RequestContext;
import com.ww.rpc.core.RpcRequest;
import com.ww.rpc.core.RpcResponse;
import com.ww.rpc.handler.RpcServerInterceptor;

/**
 * 轻量级 RPC Metrics 拦截器：统计每个 serviceName 的请求数、错误数、总耗时，
 * 不依赖外部 metrics 库，运行时可通过 {@link #snapshot()} 获取当前指标快照。
 *
 * <p>使用方式：
 * <pre>
 *   MetricsInterceptor metrics = new MetricsInterceptor();
 *   registry.addInterceptor(metrics);
 *   // ... 服务运行一段时间后
 *   Map&lt;String, Metric&gt; s = metrics.snapshot();
 * </pre>
 */
public class MetricsInterceptor implements RpcServerInterceptor {

    private final ConcurrentHashMap<String, Metric> metrics = new ConcurrentHashMap<>();

    @Override
    public RpcResponse intercept(RpcRequest request, RequestContext ctx, RpcInvoker invoker) {
        String serviceName = request.getServiceName();
        Metric m = metrics.computeIfAbsent(serviceName, k -> new Metric());
        m.requestCount.incrementAndGet();
        long start = System.nanoTime();
        try {
            RpcResponse resp = invoker.invoke(request, ctx);
            long elapsedUs = (System.nanoTime() - start) / 1000;
            m.totalLatencyUs.addAndGet(elapsedUs);
            if (resp == null || !resp.isSuccess()) {
                m.errorCount.incrementAndGet();
            }
            return resp;
        } catch (Exception e) {
            m.errorCount.incrementAndGet();
            m.totalLatencyUs.addAndGet((System.nanoTime() - start) / 1000);
            throw e;
        }
    }

    /**
     * 获取当前所有 service 的指标快照（不可变副本）。
     */
    public Map<String, Metric> snapshot() {
        Map<String, Metric> result = new ConcurrentHashMap<>();
        for (Map.Entry<String, Metric> e : metrics.entrySet()) {
            result.put(e.getKey(), e.getValue().snapshot());
        }
        return result;
    }

    /** 重置所有指标（测试/运维使用）。 */
    public void reset() {
        metrics.clear();
    }

    /**
     * 单个服务的指标数据。
     */
    public static final class Metric {
        final AtomicLong requestCount = new AtomicLong();
        final AtomicLong errorCount = new AtomicLong();
        final AtomicLong totalLatencyUs = new AtomicLong();

        Metric() {}

        private Metric(long req, long err, long latUs) {
            this.requestCount.set(req);
            this.errorCount.set(err);
            this.totalLatencyUs.set(latUs);
        }

        public long getRequestCount() { return requestCount.get(); }
        public long getErrorCount() { return errorCount.get(); }
        public long getTotalLatencyUs() { return totalLatencyUs.get(); }

        /** 平均延迟（微秒），无请求时返回 0。 */
        public double getAvgLatencyUs() {
            long req = requestCount.get();
            return req == 0 ? 0.0 : (double) totalLatencyUs.get() / req;
        }

        Metric snapshot() {
            return new Metric(requestCount.get(), errorCount.get(), totalLatencyUs.get());
        }

        @Override
        public String toString() {
            return "Metric{req=" + requestCount.get()
                    + ", err=" + errorCount.get()
                    + ", avgLatencyUs=" + String.format("%.1f", getAvgLatencyUs()) + "}";
        }
    }
}
