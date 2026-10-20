package com.ww.runtime.lb;

import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

/**
 * 基于 JDBC 的 {@link LbConfigStore}（PostgreSQL 方言，plain SQL，无 ORM）。
 *
 * <p>配置整体以 JSON 存于单行 {@code rt_lb_config(id=1)}；{@code version} 列做乐观锁，
 * JSON 内 version 与列对齐。{@link #get()} 在缺行时幂等播种默认配置；代理热路径不直接
 * 访问本类（由定时刷新缓存）。</p>
 */
public final class JdbcLbConfigStore implements LbConfigStore {

    private static final ObjectMapper JSON = JsonMapper.builder()
            // 前向兼容：库中 JSON 出现本版本未知字段时忽略而非启动失败
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    private static final int SINGLETON_ID = 1;

    private static final String SELECT_SQL = "SELECT cfg_json, version FROM rt_lb_config WHERE id = 1";

    private static final String SEED_DEFAULT_SQL = """
            INSERT INTO rt_lb_config (id, cfg_json, version, updated_at)
            VALUES (1, ?, 0, ?)
            ON CONFLICT (id) DO NOTHING
            """;

    private static final String CAS_UPDATE_SQL = """
            UPDATE rt_lb_config
            SET cfg_json = ?, version = ?, updated_at = ?
            WHERE id = 1 AND version = ?
            """;

    private final DataSource dataSource;

    public JdbcLbConfigStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    @Override
    public LbConfig get() {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(SELECT_SQL)) {
            if (rs.next()) {
                return parse(rs.getString("cfg_json"));
            }
        } catch (SQLException e) {
            throw new LbException("JdbcLbConfigStore.get failed", e);
        }
        seedDefault();
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(SELECT_SQL)) {
            if (rs.next()) {
                return parse(rs.getString("cfg_json"));
            }
            throw new LbException("rt_lb_config row missing after default seed");
        } catch (SQLException e) {
            throw new LbException("JdbcLbConfigStore.get reseed failed", e);
        }
    }

    @Override
    public LbConfig update(LbConfig next, long expectedVersion) {
        Objects.requireNonNull(next, "next");
        seedDefault(); // 极端情况下首写早于任何 get：先确保单行存在
        long newVersion = expectedVersion + 1;
        LbConfig stored = next.withVersion(newVersion);
        String json = writeJson(stored);
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(CAS_UPDATE_SQL)) {
            ps.setString(1, json);
            ps.setLong(2, newVersion);
            ps.setLong(3, System.currentTimeMillis());
            ps.setLong(4, expectedVersion);
            if (ps.executeUpdate() == 1) {
                return stored;
            }
        } catch (SQLException e) {
            throw new LbException("JdbcLbConfigStore.update failed", e);
        }
        throw new LbConfigConflictException(expectedVersion, currentVersion());
    }

    /** 缺行时插入默认配置（ON CONFLICT DO NOTHING，幂等）。 */
    private void seedDefault() {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SEED_DEFAULT_SQL)) {
            ps.setString(1, writeJson(LbConfig.defaultConfig()));
            ps.setLong(2, System.currentTimeMillis());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new LbException("JdbcLbConfigStore.seedDefault failed", e);
        }
    }

    private long currentVersion() {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(SELECT_SQL)) {
            return rs.next() ? rs.getLong("version") : -1L;
        } catch (SQLException e) {
            throw new LbException("JdbcLbConfigStore.currentVersion failed", e);
        }
    }

    private static LbConfig parse(String json) {
        try {
            return JSON.readValue(json, LbConfig.class);
        } catch (RuntimeException e) {
            throw new LbException("failed to parse rt_lb_config cfg_json", e);
        }
    }

    private static String writeJson(LbConfig config) {
        try {
            return JSON.writeValueAsString(config);
        } catch (RuntimeException e) {
            throw new LbException("failed to serialize LbConfig", e);
        }
    }
}
