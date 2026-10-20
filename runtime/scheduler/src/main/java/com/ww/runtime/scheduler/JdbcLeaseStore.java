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

    private static final String INSERT_ROW_DB_CLOCK =
            "INSERT INTO rt_scheduler_lease (id, owner, expires_at_ms) VALUES (1, ?,"
                    + " (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT + ?)"
                    + " ON CONFLICT (id) DO NOTHING";

    private static final String ACQUIRE_OR_RENEW =
            "UPDATE rt_scheduler_lease SET owner = ?, expires_at_ms = ?"
                    + " WHERE id = 1 AND (owner = ? OR expires_at_ms < ?)";

    /** S14：过期判定与写入均以数据库时钟为准（应用时钟偏移不再影响租约语义）。 */
    private static final String ACQUIRE_OR_RENEW_DB_CLOCK =
            "UPDATE rt_scheduler_lease SET owner = ?, expires_at_ms ="
                    + " (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT + ?"
                    + " WHERE id = 1 AND (owner = ?"
                    + " OR expires_at_ms < (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT)";

    private static final String SELECT_OWNER =
            "SELECT owner FROM rt_scheduler_lease WHERE id = 1 AND expires_at_ms >= ?";

    private static final String SELECT_OWNER_DB_CLOCK =
            "SELECT owner FROM rt_scheduler_lease WHERE id = 1 AND expires_at_ms >="
                    + " (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT";

    private static final String SELECT_FOR_UPDATE =
            "SELECT owner, expires_at_ms, leader_epoch FROM rt_scheduler_lease"
                    + " WHERE id = 1 FOR UPDATE";

    private static final String SELECT_FOR_UPDATE_DB_CLOCK =
            "SELECT owner, expires_at_ms, leader_epoch,"
                    + " (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT AS db_now_ms"
                    + " FROM rt_scheduler_lease WHERE id = 1 FOR UPDATE";

    private static final String UPDATE_EPOCH =
            "UPDATE rt_scheduler_lease SET owner = ?, expires_at_ms = ?, leader_epoch = ?"
                    + " WHERE id = 1";

    private static final String CURRENT_EPOCH =
            "SELECT 1 FROM rt_scheduler_lease WHERE id = 1 AND owner = ?"
                    + " AND leader_epoch = ? AND expires_at_ms >= ?";

    private static final String CURRENT_EPOCH_DB_CLOCK =
            "SELECT 1 FROM rt_scheduler_lease WHERE id = 1 AND owner = ?"
                    + " AND leader_epoch = ? AND expires_at_ms >="
                    + " (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT";

    /** 默认时钟（SystemClock）走数据库时钟分支；注入时钟（TestClock 等）保留本地时钟分支。 */
    private final boolean dbClock;

    private final DataSource dataSource;
    private final Clock clock;

    public JdbcLeaseStore(DataSource dataSource) {
        this(dataSource, SystemClock.instance());
    }

    public JdbcLeaseStore(DataSource dataSource, Clock clock) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.dbClock = clock instanceof SystemClock; // S14 分支选择
    }

    @Override
    public boolean acquireOrRenew(String owner, long ttlMillis) {
        if (owner == null || owner.isBlank()) {
            throw new IllegalArgumentException("owner must not be null or blank");
        }
        if (ttlMillis <= 0) {
            throw new IllegalArgumentException("ttlMillis must be > 0, got " + ttlMillis);
        }
        // S14：dbClock 模式下 now/expires 全由数据库求值，应用只传 ttl——
        // 多节点时钟偏移不再造成「提前判过期」或「过期后仍续约成功」
        final long now = clock.nowMillis();
        final long expiresAt = now + ttlMillis;
        try (Connection connection = dataSource.getConnection()) {
            // S20：先 UPDATE（常规续约单次往返）→ 未命中说明行不存在则 INSERT 兜底
            // → 插入冲突（行存在但条件未命中）再 UPDATE 一次（覆盖两语句间隙恰好过期）
            try (PreparedStatement update = connection.prepareStatement(
                    dbClock ? ACQUIRE_OR_RENEW_DB_CLOCK : ACQUIRE_OR_RENEW)) {
                if (dbClock) {
                    update.setString(1, owner);
                    update.setLong(2, ttlMillis);
                    update.setString(3, owner);
                } else {
                    bindAcquire(update, owner, expiresAt, now);
                }
                if (update.executeUpdate() == 1) {
                    return true;
                }
            }
            try (PreparedStatement insert = connection.prepareStatement(
                    dbClock ? INSERT_ROW_DB_CLOCK : INSERT_ROW)) {
                insert.setString(1, owner);
                insert.setLong(2, dbClock ? ttlMillis : expiresAt);
                if (insert.executeUpdate() == 1) {
                    return true; // 行首次创建成功（并发初始化只有一个节点成功）
                }
            }
            try (PreparedStatement update = connection.prepareStatement(
                    dbClock ? ACQUIRE_OR_RENEW_DB_CLOCK : ACQUIRE_OR_RENEW)) {
                if (dbClock) {
                    update.setString(1, owner);
                    update.setLong(2, ttlMillis);
                    update.setString(3, owner);
                } else {
                    bindAcquire(update, owner, expiresAt, now);
                }
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

    /**
     * 抢占/续约并返回单调 leader 纪元（P1-2 fencing token）。
     *
     * <p>事务内 {@code SELECT ... FOR UPDATE} 串行化竞争者：属主变更或过期接管时
     * {@code leader_epoch + 1}，同一属主续约保持纪元不变。返回纪元供控制面写副作用
     * 前用 {@link #isCurrent} 复核，拒绝僵尸 leader 的迟到写。</p>
     */
    @Override
    public long acquireOrRenewWithEpoch(String owner, long ttlMillis) {
        if (owner == null || owner.isBlank()) {
            throw new IllegalArgumentException("owner must not be null or blank");
        }
        if (ttlMillis <= 0) {
            throw new IllegalArgumentException("ttlMillis must be > 0, got " + ttlMillis);
        }
        try (Connection connection = dataSource.getConnection()) {
            boolean autoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                try (PreparedStatement insert = connection.prepareStatement(
                        dbClock ? INSERT_ROW_DB_CLOCK : INSERT_ROW)) {
                    insert.setString(1, owner);
                    insert.setLong(2, dbClock ? ttlMillis : clock.nowMillis() + ttlMillis);
                    insert.executeUpdate();
                }
                String currentOwner;
                long currentExpires;
                long currentEpoch;
                long now;
                try (PreparedStatement select = connection.prepareStatement(
                        dbClock ? SELECT_FOR_UPDATE_DB_CLOCK : SELECT_FOR_UPDATE);
                     ResultSet rs = select.executeQuery()) {
                    if (!rs.next()) {
                        throw new SQLException("rt_scheduler_lease row missing after insert");
                    }
                    currentOwner = rs.getString("owner");
                    currentExpires = rs.getLong("expires_at_ms");
                    currentEpoch = rs.getLong("leader_epoch");
                    now = dbClock ? rs.getLong("db_now_ms") : clock.nowMillis();
                }
                boolean sameOwner = owner.equals(currentOwner);
                boolean expired = currentExpires < now;
                if (!sameOwner && !expired) {
                    connection.rollback();
                    return -1L;
                }
                long nextEpoch = sameOwner ? currentEpoch : currentEpoch + 1;
                try (PreparedStatement update = connection.prepareStatement(UPDATE_EPOCH)) {
                    update.setString(1, owner);
                    update.setLong(2, now + ttlMillis);
                    update.setLong(3, nextEpoch);
                    update.executeUpdate();
                }
                connection.commit();
                return nextEpoch;
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(autoCommit);
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException(
                    "failed to acquire/renew scheduler lease with epoch for " + owner, e);
        }
    }

    @Override
    public boolean isCurrent(String owner, long epoch) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     dbClock ? CURRENT_EPOCH_DB_CLOCK : CURRENT_EPOCH)) {
            statement.setString(1, owner);
            statement.setLong(2, epoch);
            if (!dbClock) {
                statement.setLong(3, clock.nowMillis());
            }
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            // 无法确认时按非 leader 处理：控制面副作用宁可跳过，下一周期重试
            return false;
        }
    }

    @Override
    public Optional<String> currentOwner() {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     dbClock ? SELECT_OWNER_DB_CLOCK : SELECT_OWNER)) {
            if (!dbClock) {
                statement.setLong(1, clock.nowMillis());
            }
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next() ? Optional.of(rs.getString(1)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("failed to read scheduler lease owner", e);
        }
    }
}
