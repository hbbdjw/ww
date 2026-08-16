package com.ww.admin.entity;

import com.ww.common.entity.BaseEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

/**
 * 系统日志表：存储由 logback 采集的应用运行日志。
 * <p>
 * 日志为纯追加、无关联、无更新场景，继承 {@link BaseEntity} 复用雪花主键与 {@code create_time} 审计列；
 * {@code level / node / create_time} 建索引以支撑按级别、节点、时间段的筛选与批量删除。
 * <p>
 * 写入走 {@code BaseDaoImpl.batchPersist} 的 JDBC 批量（flush + clear），不进入业务持久化上下文。
 */
@Getter
@Setter
@Entity
@Table(name = "sys_log", indexes = {
        @Index(name = "idx_sys_log_create_time", columnList = "create_time"),
        @Index(name = "idx_sys_log_node", columnList = "node"),
        @Index(name = "idx_sys_log_level", columnList = "level")
})
public class SysLog extends BaseEntity {

    /**
     * 日志级别：TRACE / DEBUG / INFO / WARN / ERROR
     */
    @Column(name = "level", nullable = false, length = 16)
    private String level;

    /**
     * 产生日志的 Logger 名称（类全限定名）
     */
    @Column(name = "logger_name", length = 255)
    private String loggerName;

    /**
     * 产生日志的线程名
     */
    @Column(name = "thread_name", length = 128)
    private String threadName;

    /**
     * 日志消息正文
     */
    @Column(name = "message", nullable = false, columnDefinition = "text")
    private String message;

    /**
     * 异常堆栈（无异常时为 null）
     */
    @Column(name = "stack_trace", columnDefinition = "text")
    private String stackTrace;

    /**
     * 集群节点标识（host:port）
     */
    @Column(name = "node", length = 64)
    private String node;
}
