package com.ww.runtime.scheduler;

import com.ww.runtime.common.Clock;
import com.ww.runtime.common.RuntimeFrameworkException;
import com.ww.runtime.common.SystemClock;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;

/**
 * {@link LeaseStore} 的 PostgreSQL 实现：单行表 {@code rt_scheduler_lease}
 * （{@code id} 恒为 1，见 {@code sql/rt_scheduler.sql}，使用前须
 * {@link SchedulerSchema#bootstrap(DataSource)} 建表）。
 *
 * <p>抢占用条件 UPDATE 的 affected-row 判断：</p>
 * <pre>
 *   UPDATE rt_scheduler_lease SET owner = ?, expires_at_ms = ?
 *    WHERE id = 1 AND (owner = ? OR expires_at_ms &lt; ?)
 * </pre>
 * <p>常规续约路径单次 UPDATE 往返（S20）；行不存在时先 {@code INSERT ...
 * ON CONFLICT (id) DO NOTHING} 兜底（并发初始化只有一个节点插入成功），
 * 插入冲突（行存在但条件未命中）再 UPDATE 一次覆盖两语句间隙恰好过期。
 * 全部语义原子完成，多实例并发竞争安全。</p>
 *
 * <p>过期语义：{@code expires_at_ms < now}（与 {@link InMemoryLeaseStore} 一致）。
 * SQL 异常包装为 {@link RuntimeFrameworkException}。</p>
 *
 * <p>针对真实 PostgreSQL 的行为（条件 UPDATE 并发抢占等）由集成测试覆盖。</p>
 */
public final class JdbcLeaseStore implements LeaseStore {

    private static final String INSERT_ROW =
            "INSERT INTO rt_scheduler_lease (id, owner, expires_at_ms) VALUES (1, ?, ?)"
                    + " ON CONFLICT (id) DO NOTHING";

    private static final String ACQUIRE_OR_RENEW =
            "UPDATE rt_scheduler_lease SET owner = ?, expires_at_ms = ?"
                    + " WHERE id = 1 AND (owner = ? OR expires_at_ms < ?)";

    private static final String SELECT_OWNER =
            "SELECT owner FROM rt_scheduler_lease WHERE id = 1 AND expires_at_ms >= ?";

    private final DataSource dataSource;
    private final Clock clock;

    public JdbcLeaseStore(DataSource dataSource) {
        this(dataSource, SystemClock.instance());
    }

    public JdbcLeaseStore(DataSource dataSource, Clock clock) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public boolean acquireOrRenew(String owner, long ttlMillis) {
        if (owner == null || owner.isBlank()) {
            throw new IllegalArgumentException("owner must not be null or blank");
        }
        if (ttlMillis <= 0) {
            throw new IllegalArgumentException("ttlMillis must be > 0, got " + ttlMillis);
        }
        long now = clock.nowMillis();
        long expiresAt = now + ttlMillis;
        try (Connection connection = dataSource.getConnection()) {
            // S20：先 UPDATE（常规续约单次往返）→ 未命中说明行不存在则 INSERT 兜底
            // → 插入冲突（行存在但条件未命中）再 UPDATE 一次（覆盖两语句间隙恰好过期）
            try (PreparedStatement update = connection.prepareStatement(ACQUIRE_OR_RENEW)) {
                bindAcquire(update, owner, expiresAt, now);
                if (update.executeUpdate() == 1) {
                    return true;
                }
            }
            try (PreparedStatement insert = connection.prepareStatement(INSERT_ROW)) {
                insert.setString(1, owner);
                insert.setLong(2, expiresAt);
                if (insert.executeUpdate() == 1) {
                    return true; // 行首次创建成功（并发初始化只有一个节点成功）
                }
            }
            try (PreparedStatement update = connection.prepareStatement(ACQUIRE_OR_RENEW)) {
                bindAcquire(update, owner, expiresAt, now);
                return update.executeUpdate() == 1;
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("failed to acquire/renew scheduler lease for " + owner, e);
        }
    }

    private static void bindAcquire(PreparedStatement statement, String owner, long expiresAt, long now)
            throws SQLException {
        statement.setString(1, owner);
        statement.setLong(2, expiresAt);
        statement.setString(3, owner);
        statement.setLong(4, now);
    }

    @Override
    public Optional<String> currentOwner() {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(SELECT_OWNER)) {
            statement.setLong(1, clock.nowMillis());
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next() ? Optional.of(rs.getString(1)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("failed to read scheduler lease owner", e);
        }
    }
}
