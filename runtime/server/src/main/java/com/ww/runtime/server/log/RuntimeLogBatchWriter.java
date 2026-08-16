package com.ww.runtime.server.log;

import com.ww.runtime.common.NodeId;
import com.ww.runtime.log.DbLogAppender;
import com.ww.runtime.log.LogBuffer;
import com.ww.runtime.log.LogBus;
import com.ww.runtime.log.LogRecord;
import com.ww.runtime.server.RuntimeProperties;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 日志批量落库 + 实时广播线程：从 {@link LogBuffer} 批量取出日志，
 * 落库到 {@code rt_log}（jdbc 模式）并广播给 {@link LogBus} 订阅者。
 *
 * <p>单守护线程循环：阻塞等待至多 {@code flush-interval-ms} 取第一批，随后尽量凑满
 * {@code batch-size} 条。落库完成后把本批日志广播给实时订阅者。</p>
 *
 * <p>{@code runtime.log.enabled=false} 时线程仍消费队列但跳过落库，避免队列无限堆积；
 * memory 模式（无 DataSource）同样跳过落库仅广播。采集与落库解耦，业务线程绝不因日志写库阻塞。</p>
 *
 * <p><b>防死循环</b>：落库期间 {@link DbLogAppender#setSuppressed(boolean)} 抑制本线程采集，
 * 打断「写库 → SQL 日志 → 采集 → 再写库」循环；广播阶段不抑制（不做 SQL，
 * batch-writer 线程自身日志由线程名过滤兜底）。</p>
 */
@Service
public class RuntimeLogBatchWriter {

    private static final Logger log = LoggerFactory.getLogger(RuntimeLogBatchWriter.class);

    private final ObjectProvider<DataSource> dataSource;
    private final LogBus logBus;
    private final NodeId self;
    private final RuntimeProperties props;

    private volatile JdbcLogStore logStore;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile Thread workerThread;

    public RuntimeLogBatchWriter(ObjectProvider<DataSource> dataSource,
                                 LogBus logBus,
                                 NodeId self,
                                 RuntimeProperties props) {
        this.dataSource = dataSource;
        this.logBus = logBus;
        this.self = self;
        this.props = props;
    }

    @PostConstruct
    public void start() {
        RuntimeProperties.Log cfg = props.log();
        LogBuffer.configure(cfg.queueCapacity());
        DbLogAppender.setNodeKey(self.value());

        DataSource ds = dataSource.getIfAvailable();
        if (ds != null) {
            logStore = new JdbcLogStore(ds);
        }

        running.set(true);
        workerThread = Thread.ofPlatform().daemon(true)
                .name("log-batch-writer")
                .start(this::runLoop);
        log.info("runtime log batch writer started: enabled={}, persist={}, batchSize={}, flushIntervalMs={}, queueCapacity={}",
                cfg.enabled(), logStore != null, cfg.batchSize(), cfg.flushIntervalMs(), cfg.queueCapacity());
    }

    private void runLoop() {
        RuntimeProperties.Log cfg = props.log();
        while (running.get()) {
            List<LogRecord> batch = LogBuffer.pollBatch(cfg.batchSize(), cfg.flushIntervalMs());
            if (batch.isEmpty()) {
                continue;
            }
            if (cfg.enabled() && logStore != null) {
                try {
                    persist(batch);
                } catch (Exception e) {
                    log.warn("runtime log persist failed, dropping {} records: {}", batch.size(), e.getMessage());
                }
            }
            // 实时广播：不做 SQL 无需抑制；batch-writer 线程自身日志由线程名过滤兜底
            try {
                logBus.publish(batch);
            } catch (Exception e) {
                log.debug("runtime log publish failed: {}", e.getMessage());
            }
        }
        // 停机前尽量刷出剩余日志
        drainRemaining();
    }

    /** 批量落库；期间抑制本线程日志采集，防写库自引用循环。 */
    private void persist(List<LogRecord> batch) {
        DbLogAppender.setSuppressed(true);
        try {
            logStore.batchInsert(batch);
        } finally {
            DbLogAppender.setSuppressed(false);
        }
    }

    /** 停机时非阻塞 drain 剩余日志并落库。 */
    private void drainRemaining() {
        RuntimeProperties.Log cfg = props.log();
        List<LogRecord> remaining = LogBuffer.pollBatch(Integer.MAX_VALUE, 0);
        while (!remaining.isEmpty()) {
            if (cfg.enabled() && logStore != null) {
                try {
                    persist(remaining);
                } catch (Exception e) {
                    log.warn("runtime log drain on stop failed, dropping {} records: {}", remaining.size(), e.getMessage());
                }
            }
            try {
                logBus.publish(remaining);
            } catch (Exception ignore) {
                // 停机阶段订阅者可能已关闭
            }
            remaining = LogBuffer.pollBatch(Integer.MAX_VALUE, 0);
        }
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
        log.info("runtime log batch writer stopped, total dropped {}", LogBuffer.droppedCount());
    }
}
