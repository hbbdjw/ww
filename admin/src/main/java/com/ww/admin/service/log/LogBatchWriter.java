package com.ww.admin.service.log;

import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import com.ww.admin.entity.SysLog;
import com.ww.cache.ClusterMembership;
import com.ww.common.ClusterNode;
import com.ww.common.ClusterProperties;
import com.ww.common.dao.BaseDaoImpl;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

/**
 * 日志批量落库线程：从 {@link LogBuffer} 批量取出日志，用 JPA {@code batchPersist}（flush + clear）
 * 走 JDBC 批量写入 {@code sys_log} 表。
 * <p>
 * 单守护线程循环：阻塞等待至多 {@code flush-interval-ms} 取第一批，随后尽量凑满 {@code batch-size} 条；
 * 写入完成后（todo：实时推送阶段）把本批日志广播给实时订阅者。
 * <p>
 * {@code log.enabled=false} 时线程仍消费队列但跳过落库，避免队列无限堆积；采集与落库解耦，
 * 业务线程绝不因日志写库而阻塞。
 */
@Service
public class LogBatchWriter {

    private static final Logger log = LoggerFactory.getLogger(LogBatchWriter.class);

    private final BaseDaoImpl baseDao;
    private final PlatformTransactionManager transactionManager;
    private final LogStreamService logStreamService;

    @Autowired(required = false)
    private ClusterMembership membership;

    @Autowired(required = false)
    private ClusterProperties clusterProperties;

    /** 是否启用数据库日志采集落库。 */
    @Value("${log.enabled:true}")
    private boolean enabled;

    /** 每批最多写入条数。 */
    @Value("${log.batch-size:200}")
    private int batchSize;

    /** 队列为空时阻塞等待毫秒数（超时即刷出当前批）。 */
    @Value("${log.flush-interval-ms:1000}")
    private long flushIntervalMs;

    /** 内存队列容量。 */
    @Value("${log.queue-capacity:10000}")
    private int queueCapacity;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile Thread workerThread;

    public LogBatchWriter(BaseDaoImpl baseDao, PlatformTransactionManager transactionManager,
                          LogStreamService logStreamService) {
        this.baseDao = baseDao;
        this.transactionManager = transactionManager;
        this.logStreamService = logStreamService;
    }

    @PostConstruct
    public void start() {
        LogBuffer.configure(queueCapacity);
        DbLogAppender.setNodeKey(resolveSelfNodeKey());

        running.set(true);
        workerThread = Thread.ofPlatform().daemon(true)
                .name("log-batch-writer")
                .start(this::runLoop);
        log.info("日志批量写入线程已启动：enabled={}, batchSize={}, flushIntervalMs={}, queueCapacity={}",
                enabled, batchSize, flushIntervalMs, queueCapacity);
    }

    private void runLoop() {
        while (running.get()) {
            List<LogRecord> batch = LogBuffer.pollBatch(batchSize, flushIntervalMs);
            if (batch.isEmpty()) {
                continue;
            }
            if (enabled) {
                try {
                    persist(batch);
                } catch (Exception e) {
                    log.warn("批量写日志失败，丢弃 {} 条：{}", batch.size(), e.getMessage());
                }
            }
            // 实时推送：publish 阶段不会执行 SQL，无需抑制；batch-writer 线程自身产生的日志
            // 由 DbLogAppender 按线程名前过滤（log-batch-writer），避免采集队列回流自身输出。
            try {
                logStreamService.publish(batch);
            } catch (Exception e) {
                log.debug("实时日志推送失败：{}", e.getMessage());
            }
        }
        // 停机前尽量刷出剩余日志
        drainRemaining();
    }

    /** 批量落库（独立事务，每批 flush + clear 释放持久化上下文）。落库期间抑制本线程日志采集，防写库自引用循环。 */
    private void persist(List<LogRecord> batch) {
        List<SysLog> entities = batch.stream().map(this::toEntity).toList();
        DbLogAppender.setSuppressed(true);
        try {
            new TransactionTemplate(transactionManager).executeWithoutResult(status ->
                    baseDao.batchPersist(entities, batchSize));
        } finally {
            DbLogAppender.setSuppressed(false);
        }
    }

    /** 将采集记录转为实体；create_time 写回日志真实产生时刻。 */
    private SysLog toEntity(LogRecord r) {
        SysLog entity = new SysLog();
        entity.setLevel(r.level());
        entity.setLoggerName(r.loggerName());
        entity.setThreadName(r.threadName());
        entity.setMessage(r.message());
        entity.setStackTrace(r.stackTrace());
        entity.setNode(r.node());
        entity.setCreatedDate(new Date(r.timestamp()));
        return entity;
    }

    /** 停机时非阻塞 drain 剩余日志并落库。 */
    private void drainRemaining() {
        List<LogRecord> remaining = LogBuffer.pollBatch(Integer.MAX_VALUE, 0);
        while (!remaining.isEmpty()) {
            if (enabled) {
                try {
                    persist(remaining);
                } catch (Exception e) {
                    log.warn("停机刷出日志失败，丢弃 {} 条：{}", remaining.size(), e.getMessage());
                }
            }
            remaining = LogBuffer.pollBatch(Integer.MAX_VALUE, 0);
        }
    }

    /**
     * 解析本机节点标识（host:port）：优先动态 membership，退化为静态配置中 self 节点。
     */
    private String resolveSelfNodeKey() {
        if (membership != null) {
            String key = membership.getSelfNodeKey();
            if (key != null && !key.isEmpty()) {
                return key;
            }
        }
        if (clusterProperties != null && clusterProperties.getNodes() != null) {
            for (ClusterNode n : clusterProperties.getNodes()) {
                if (n.isSelf()) {
                    return n.nodeKey();
                }
            }
        }
        return null;
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        Thread t = workerThread;
        if (t != null) {
            t.interrupt();
            try {
                t.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        log.info("日志批量写入线程已停止，累计丢弃 {} 条", LogBuffer.droppedCount());
    }
}
