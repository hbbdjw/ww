package com.ww.common;

import com.ww.common.entity.BaseEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

/**
 * AOF 持久化日志条目，记录每一次写操作。
 * <p>
 * 主键改用 {@link com.ww.common.id.SnowflakeId}（继承自 {@link BaseEntity}），
 * 非 IDENTITY 以启用 JDBC batch insert。
 */
@Entity
@Table(name = "aof_entry")
@Getter
@Setter
public class AofEntry extends BaseEntity {

    /** 操作类型：PUT / REMOVE / CLEAR / HSET / LPUSH / RPUSH / LPOP / RPOP / SADD / SREM / ZADD */
    @Column(nullable = false, length = 32)
    private String operation;

    /** 操作的缓存键 */
    @Column(name = "cache_key", nullable = false, length = 512)
    private String cacheKey;

    /** JSON 序列化的参数 */
    @Column(columnDefinition = "TEXT")
    private String args;

    public AofEntry() {
    }

    public AofEntry(String operation, String cacheKey, String args) {
        this.operation = operation;
        this.cacheKey = cacheKey;
        this.args = args;
    }
}
