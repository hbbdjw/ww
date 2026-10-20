package com.ww.runtime.placement;

import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.PlacementRole;
import com.ww.runtime.common.RuntimeFrameworkException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * 基于 JDBC 的 {@link PlacementStore} 实现（PostgreSQL 方言，plain SQL，无 ORM）。
 *
 * <p>不引入连接池依赖：{@link DataSource} 由宿主注入。建表 DDL 见
 * {@code src/main/resources/sql/rt_placement.sql}（v2 主键
 * {@code (capability, partition_id, node_id)}，每分区多行），可通过
 * {@link PlacementSchema#bootstrap(DataSource)} 幂等初始化。</p>
 *
 * <p>语义要点：</p>
 * <ul>
 *   <li>replaceAll：单事务内「DELETE 该 capability 全部行 + 批量 INSERT」，
 *       提交前对读者不可见，失败整体回滚（原子整体替换）；每分区至多一条 PRIMARY。</li>
 *   <li>cutover：单条 UPDATE 的条件即 CAS——
 *       {@code WHERE capability=? AND partition_id=? AND epoch=? AND node_id<>?}，
 *       命中 0 行即竞争失败，返回 false（仅 PRIMARY 行生效）。</li>
 *   <li>promoteReplica：事务内「SELECT FOR UPDATE 锁分区全部行 → 校验 PRIMARY 行
 *       epoch + REPLICA 行存在 → DELETE 原 PRIMARY 行 + UPDATE 副本行升主（epoch+1）」。</li>
 *   <li>reassignReplicas：事务内「DELETE 该分区 REPLICA 行 + INSERT 新副本行
 *       （epoch 对齐 PRIMARY 行）」，不动主不动 epoch。</li>
 * </ul>
 *
 * <p><b>测试说明：</b>本模块单测无 PostgreSQL 环境，仅验证语义自洽；
 * 针对真实 PostgreSQL 的行为（事务原子性、cutover 并发竞争、Schema 幂等建表等）
 * 由集成测试覆盖。</p>
 */
public final class JdbcPlacementStore implements PlacementStore {

    private static final String DELETE_SQL =
            "DELETE FROM rt_partition_placement WHERE capability = ?";

    private static final String INSERT_SQL = """
            INSERT INTO rt_partition_placement
                (capability, partition_id, node_id, role, epoch, valid_after_ms, lease_until_ms, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private static final String SELECT_COLUMNS = "partition_id, node_id, role, epoch, valid_after_ms";

    private static final String SELECT_ALL_SQL = """
            SELECT %s FROM rt_partition_placement WHERE capability = ?
            ORDER BY partition_id, role ASC, node_id
            """.formatted(SELECT_COLUMNS);

    private static final String SELECT_PARTITION_SQL = """
            SELECT %s FROM rt_partition_placement
            WHERE capability = ? AND partition_id = ?
            ORDER BY role ASC, node_id
            """.formatted(SELECT_COLUMNS);

    /** S6：主行直查（角色下推 WHERE）。 */
    private static final String SELECT_PRIMARIES_SQL = """
            SELECT partition_id, node_id FROM rt_partition_placement
            WHERE capability = ? AND role = 'PRIMARY'
            ORDER BY partition_id
            """;

    private static final String SELECT_PARTITION_FOR_UPDATE_SQL = """
            SELECT node_id, role, epoch, lease_until_ms FROM rt_partition_placement
            WHERE capability = ? AND partition_id = ?
            ORDER BY role ASC, node_id
            FOR UPDATE
            """;

    /**
     * 单条 CAS 切主：新属主的 valid_after = max(数据库当前时间, 旧属主租约到期) + 安全余量。
     * SET 表达式读取的是旧行值，因此 lease_until_ms 即旧主最后续约的到期时间。
     */
    private static final String CUTOVER_SQL = """
            UPDATE rt_partition_placement
            SET node_id = ?, epoch = epoch + 1, updated_at = ?,
                lease_until_ms = 0,
                valid_after_ms = GREATEST((EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT, lease_until_ms) + ?
            WHERE capability = ? AND partition_id = ? AND role = 'PRIMARY' AND epoch = ? AND node_id <> ?
            """;

    private static final String DELETE_PRIMARY_SQL = """
            DELETE FROM rt_partition_placement
            WHERE capability = ? AND partition_id = ? AND role = 'PRIMARY' AND node_id = ?
            """;

    /**
     * 副本提升：新属主 valid_after = max(数据库当前时间, 旧主租约到期) + 安全余量。
     * 旧主行在提升前删除，其租约由读取到的 lease_until_ms 传入。
     */
    private static final String PROMOTE_SQL = """
            UPDATE rt_partition_placement
            SET role = 'PRIMARY', epoch = epoch + 1, updated_at = ?,
                lease_until_ms = 0,
                valid_after_ms = GREATEST((EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT, ?) + ?
            WHERE capability = ? AND partition_id = ? AND role = 'REPLICA' AND node_id = ?
            """;

    private static final String DELETE_REPLICAS_SQL = """
            DELETE FROM rt_partition_placement
            WHERE capability = ? AND partition_id = ? AND role = 'REPLICA'
            """;

    private static final String SELECT_REVISION_SQL =
            "SELECT revision FROM rt_placement_revision WHERE capability = ?";

    /** 数据库时钟：valid_after_ms / lease_until_ms 的唯一时间基准。 */
    private static final String SELECT_DB_NOW_SQL =
            "SELECT (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT";

    /**
     * 全量替换前锁定并读取旧租约上界：防止“读上界 → 旧属主续约 → 删除插入”的竞态
     * 让新属主早于旧租约到期开始写入。
     */
    private static final String SELECT_MAX_LEASE_SQL = """
            SELECT lease_until_ms FROM rt_partition_placement
            WHERE capability = ?
            ORDER BY partition_id, node_id
            FOR UPDATE
            """;

    private static final String BUMP_REVISION_SQL = """
            INSERT INTO rt_placement_revision (capability, revision, updated_at)
            VALUES (?, 1, ?)
            ON CONFLICT (capability) DO UPDATE
            SET revision = rt_placement_revision.revision + 1,
                updated_at = EXCLUDED.updated_at
            """;

    /**
     * 批量续约：仅续约仍由 owner 持有、epoch 匹配且 valid_after 已到达的 PRIMARY 行。
     * 返回 partition → lease_until_ms；调用方只用 keySet 判断续约成功。
     */
    private static final String RENEW_LEASES_SQL_TEMPLATE = """
            UPDATE rt_partition_placement
            SET lease_until_ms = GREATEST(
                    lease_until_ms, (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT + ?)
            WHERE capability = ? AND role = 'PRIMARY' AND node_id = ? AND epoch = ?
              AND valid_after_ms <= (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT
              AND partition_id IN (%s)
            RETURNING partition_id, lease_until_ms
            """;

    /** 释放租约（优雅停机）：仅释放仍由 owner 持有且 epoch 匹配的行。 */
    private static final String RELEASE_LEASES_SQL_TEMPLATE = """
            UPDATE rt_partition_placement
            SET lease_until_ms = 0
            WHERE capability = ? AND role = 'PRIMARY' AND node_id = ? AND epoch = ?
              AND partition_id IN (%s)
            """;

    /** 切主后新属主等待旧租约过期的安全余量（数据库时间）。 */
    static final long CUTOVER_SAFETY_MARGIN_MS = 1_000L;

    private final DataSource dataSource;

    public JdbcPlacementStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    @Override
    public void replaceAll(String capability, List<Placement> placements) {
        Objects.requireNonNull(capability, "capability");
        Objects.requireNonNull(placements, "placements");
        Set<Integer> primaryPartitions = new LinkedHashSet<>();
        for (Placement placement : placements) {
            Objects.requireNonNull(placement, "placement");
            if (placement.isPrimary() && !primaryPartitions.add(placement.partition())) {
                throw new IllegalArgumentException(
                        "duplicate PRIMARY placement for partition " + placement.partition());
            }
        }
        try (Connection conn = dataSource.getConnection()) {
            boolean oldAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                long now = System.currentTimeMillis();
                long maxLeaseUntil = maxLeaseUntil(conn, capability);
                // 全量替换可能改变属主：新属主必须等旧属主租约过期后再写。
                // 无活动租约（首次引导/旧版本升级）不额外延迟。
                long validAfter = maxLeaseUntil > 0
                        ? Math.max(now, maxLeaseUntil) + CUTOVER_SAFETY_MARGIN_MS
                        : 0L;
                try (PreparedStatement del = conn.prepareStatement(DELETE_SQL)) {
                    del.setString(1, capability);
                    del.executeUpdate();
                }
                try (PreparedStatement ins = conn.prepareStatement(INSERT_SQL)) {
                    for (Placement placement : placements) {
                        ins.setString(1, capability);
                        ins.setInt(2, placement.partition());
                        ins.setString(3, placement.node().value());
                        ins.setString(4, placement.role().name());
                        ins.setLong(5, placement.epoch());
                        ins.setLong(6, Math.max(placement.validAfterMs(), validAfter));
                        ins.setLong(7, 0L); // 新引导的放置由属主启动后自行续约
                        ins.setLong(8, now);
                        ins.addBatch();
                    }
                    ins.executeBatch();
                }
                bumpRevision(conn, capability, now);
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(oldAutoCommit);
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException(
                    "JdbcPlacementStore.replaceAll failed for capability " + capability, e);
        }
    }

    @Override
    public List<Placement> list(String capability) {
        Objects.requireNonNull(capability, "capability");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_ALL_SQL)) {
            ps.setString(1, capability);
            try (ResultSet rs = ps.executeQuery()) {
                List<Placement> result = new ArrayList<>();
                while (rs.next()) {
                    result.add(mapRow(rs));
                }
                return result;
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException(
                    "JdbcPlacementStore.list failed for capability " + capability, e);
        }
    }

    @Override
    public Optional<Placement> get(String capability, int partition) {
        List<Placement> rows = placements(capability, partition);
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.getFirst());
    }

    @Override
    public List<Placement> placements(String capability, int partition) {
        Objects.requireNonNull(capability, "capability");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_PARTITION_SQL)) {
            ps.setString(1, capability);
            ps.setInt(2, partition);
            try (ResultSet rs = ps.executeQuery()) {
                List<Placement> result = new ArrayList<>();
                while (rs.next()) {
                    result.add(mapRow(rs));
                }
                return result;
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException(
                    "JdbcPlacementStore.placements failed for capability " + capability
                            + " partition " + partition, e);
        }
    }

    @Override
    public Map<Integer, NodeId> primaries(String capability) {
        // S6：角色过滤下推 WHERE（旧实现拉全量行内存筛——每周期 × 能力 × P×rf 行读放大）
        Objects.requireNonNull(capability, "capability");
        Map<Integer, NodeId> primaries = new LinkedHashMap<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_PRIMARIES_SQL)) {
            ps.setString(1, capability);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    primaries.put(rs.getInt("partition_id"), NodeId.of(rs.getString("node_id")));
                }
            }
            return primaries;
        } catch (SQLException e) {
            throw new RuntimeFrameworkException(
                    "JdbcPlacementStore.primaries failed for capability " + capability, e);
        }
    }

    @Override
    public boolean cutover(String capability, int partition, NodeId newNode, long expectedEpoch) {
        Objects.requireNonNull(capability, "capability");
        Objects.requireNonNull(newNode, "newNode");
        try (Connection conn = dataSource.getConnection()) {
            boolean oldAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                boolean success;
                try (PreparedStatement ps = conn.prepareStatement(CUTOVER_SQL)) {
                    ps.setString(1, newNode.value());
                    ps.setLong(2, System.currentTimeMillis());
                    ps.setLong(3, CUTOVER_SAFETY_MARGIN_MS);
                    ps.setString(4, capability);
                    ps.setInt(5, partition);
                    ps.setLong(6, expectedEpoch);
                    ps.setString(7, newNode.value());
                    success = ps.executeUpdate() > 0;
                }
                if (success) {
                    bumpRevision(conn, capability, System.currentTimeMillis());
                    conn.commit();
                } else {
                    conn.rollback();
                }
                return success;
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(oldAutoCommit);
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException(
                    "JdbcPlacementStore.cutover failed for capability " + capability
                            + " partition " + partition, e);
        }
    }

    @Override
    public boolean promoteReplica(String capability, int partition, NodeId newNode, long expectedEpoch) {
        Objects.requireNonNull(capability, "capability");
        Objects.requireNonNull(newNode, "newNode");
        try (Connection conn = dataSource.getConnection()) {
            boolean oldAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            boolean success = false;
            try {
                // 锁定分区全部行：与并发 cutover/promote/reassign 串行化
                NodeId primaryNode = null;
                long primaryEpoch = -1L;
                long primaryLeaseUntil = 0L;
                boolean hasReplicaOnNewNode = false;
                try (PreparedStatement lock = conn.prepareStatement(SELECT_PARTITION_FOR_UPDATE_SQL)) {
                    lock.setString(1, capability);
                    lock.setInt(2, partition);
                    try (ResultSet rs = lock.executeQuery()) {
                        while (rs.next()) {
                            boolean isPrimary = "PRIMARY".equals(rs.getString("role"));
                            NodeId node = NodeId.of(rs.getString("node_id"));
                            if (isPrimary) {
                                primaryNode = node;
                                primaryEpoch = rs.getLong("epoch");
                                primaryLeaseUntil = rs.getLong("lease_until_ms");
                            } else if (node.equals(newNode)) {
                                hasReplicaOnNewNode = true;
                            }
                        }
                    }
                }
                if (primaryNode != null && primaryEpoch == expectedEpoch && hasReplicaOnNewNode) {
                    long now = System.currentTimeMillis();
                    try (PreparedStatement del = conn.prepareStatement(DELETE_PRIMARY_SQL)) {
                        del.setString(1, capability);
                        del.setInt(2, partition);
                        del.setString(3, primaryNode.value());
                        del.executeUpdate();
                    }
                    try (PreparedStatement promote = conn.prepareStatement(PROMOTE_SQL)) {
                        promote.setLong(1, now);
                        promote.setLong(2, primaryLeaseUntil);
                        promote.setLong(3, CUTOVER_SAFETY_MARGIN_MS);
                        promote.setString(4, capability);
                        promote.setInt(5, partition);
                        promote.setString(6, newNode.value());
                        promote.executeUpdate();
                    }
                    bumpRevision(conn, capability, now);
                    success = true;
                    conn.commit();
                } else {
                    conn.rollback();
                }
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(oldAutoCommit);
            }
            return success;
        } catch (SQLException e) {
            throw new RuntimeFrameworkException(
                    "JdbcPlacementStore.promoteReplica failed for capability " + capability
                            + " partition " + partition, e);
        }
    }

    @Override
    public void reassignReplicas(String capability, int partition, List<NodeId> newReplicas) {
        Objects.requireNonNull(capability, "capability");
        Objects.requireNonNull(newReplicas, "newReplicas");
        try (Connection conn = dataSource.getConnection()) {
            boolean oldAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                reassignInTx(conn, capability, partition, newReplicas);
                bumpRevision(conn, capability, System.currentTimeMillis());
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(oldAutoCommit);
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException(
                    "JdbcPlacementStore.reassignReplicas failed for capability " + capability
                            + " partition " + partition, e);
        }
    }

    /** S8：适度批量上限——32 分区/事务（控制行锁持有时长与失败粒度，不做超大事务）。 */
    private static final int REASSIGN_BATCH_PER_TX = 32;

    @Override
    public void reassignReplicasBatch(String capability, Map<Integer, List<NodeId>> fixes) {
        Objects.requireNonNull(capability, "capability");
        if (fixes == null || fixes.isEmpty()) {
            return;
        }
        List<Map.Entry<Integer, List<NodeId>>> entries = new ArrayList<>(fixes.entrySet());
        try (Connection conn = dataSource.getConnection()) {
            boolean oldAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                for (int i = 0; i < entries.size(); i++) {
                    reassignInTx(conn, capability, entries.get(i).getKey(), entries.get(i).getValue());
                    if ((i + 1) % REASSIGN_BATCH_PER_TX == 0 || i == entries.size() - 1) {
                        bumpRevision(conn, capability, System.currentTimeMillis());
                        conn.commit(); // 每 ≤32 分区一提交
                    }
                }
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(oldAutoCommit);
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException(
                    "JdbcPlacementStore.reassignReplicasBatch failed for capability " + capability
                            + " (" + entries.size() + " partitions)", e);
        }
    }

    /** 单分区副本集重置（须在事务内调用）：锁分区行 → 取主 epoch → 删旧副本 → 插新副本。 */
    private static void reassignInTx(Connection conn, String capability, int partition,
                                     List<NodeId> newReplicas) throws SQLException {
        long primaryEpoch = 0L;
        NodeId primaryNode = null;
        try (PreparedStatement lock = conn.prepareStatement(SELECT_PARTITION_FOR_UPDATE_SQL)) {
            lock.setString(1, capability);
            lock.setInt(2, partition);
            try (ResultSet rs = lock.executeQuery()) {
                while (rs.next()) {
                    if ("PRIMARY".equals(rs.getString("role"))) {
                        primaryNode = NodeId.of(rs.getString("node_id"));
                        primaryEpoch = rs.getLong("epoch");
                    }
                }
            }
        }
        if (primaryNode == null) {
            return; // 分区不存在：无主可依附，忽略（与单分区路径语义一致）
        }
        Set<NodeId> distinct = new LinkedHashSet<>(newReplicas);
        distinct.remove(primaryNode); // 主节点不兼任副本
        long now = System.currentTimeMillis();
        try (PreparedStatement del = conn.prepareStatement(DELETE_REPLICAS_SQL)) {
            del.setString(1, capability);
            del.setInt(2, partition);
            del.executeUpdate();
        }
        try (PreparedStatement ins = conn.prepareStatement(INSERT_SQL)) {
            for (NodeId node : distinct) {
                ins.setString(1, capability);
                ins.setInt(2, partition);
                ins.setString(3, node.value());
                ins.setString(4, PlacementRole.REPLICA.name());
                ins.setLong(5, primaryEpoch); // epoch 对齐主行（不动主不动 epoch）
                ins.setLong(6, 0L);           // 副本不参与写 fencing
                ins.setLong(7, 0L);
                ins.setLong(8, now);
                ins.addBatch();
            }
            ins.executeBatch();
        }
    }

    @Override
    public long revision(String capability) {
        Objects.requireNonNull(capability, "capability");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_REVISION_SQL)) {
            ps.setString(1, capability);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0L;
            }
        } catch (SQLException e) {
            throw new RuntimeFrameworkException(
                    "JdbcPlacementStore.revision failed for capability " + capability, e);
        }
    }

    @Override
    public long dbNowMillis() {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_DB_NOW_SQL);
             ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getLong(1) : -1L;
        } catch (SQLException e) {
            throw new RuntimeFrameworkException("JdbcPlacementStore.dbNowMillis failed", e);
        }
    }

    @Override
    public Map<Integer, Long> renewLeases(String capability, List<Integer> partitions,
                                          NodeId owner, long epoch, long ttlMillis) {
        Objects.requireNonNull(capability, "capability");
        Objects.requireNonNull(partitions, "partitions");
        Objects.requireNonNull(owner, "owner");
        if (ttlMillis <= 0) {
            throw new IllegalArgumentException("ttlMillis must be > 0, got " + ttlMillis);
        }
        if (partitions.isEmpty()) {
            return Map.of();
        }
        String sql = RENEW_LEASES_SQL_TEMPLATE.formatted(placeholders(partitions.size()));
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            int index = 1;
            ps.setLong(index++, ttlMillis);
            ps.setString(index++, capability);
            ps.setString(index++, owner.value());
            ps.setLong(index++, epoch);
            for (Integer partition : partitions) {
                ps.setInt(index++, partition);
            }
            Map<Integer, Long> renewed = new LinkedHashMap<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    renewed.put(rs.getInt("partition_id"), rs.getLong("lease_until_ms"));
                }
            }
            return renewed;
        } catch (SQLException e) {
            throw new RuntimeFrameworkException(
                    "JdbcPlacementStore.renewLeases failed for capability " + capability
                            + " owner " + owner, e);
        }
    }

    @Override
    public void releaseLeases(String capability, List<Integer> partitions, NodeId owner) {
        Objects.requireNonNull(capability, "capability");
        Objects.requireNonNull(partitions, "partitions");
        Objects.requireNonNull(owner, "owner");
        if (partitions.isEmpty()) {
            return;
        }
        String sql = RELEASE_LEASES_SQL_TEMPLATE.formatted(placeholders(partitions.size()));
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            int index = 1;
            ps.setString(index++, capability);
            ps.setString(index++, owner.value());
            for (Integer partition : partitions) {
                ps.setInt(index++, partition);
            }
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeFrameworkException(
                    "JdbcPlacementStore.releaseLeases failed for capability " + capability
                            + " owner " + owner, e);
        }
    }

    private static long maxLeaseUntil(Connection conn, String capability) throws SQLException {
        long max = 0L;
        try (PreparedStatement ps = conn.prepareStatement(SELECT_MAX_LEASE_SQL)) {
            ps.setString(1, capability);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    max = Math.max(max, rs.getLong(1));
                }
            }
        }
        return max;
    }

    /** 单事务内推进 capability 修订号（与放置变更同提交，缓存据此重载）。 */
    private static void bumpRevision(Connection conn, String capability, long now) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(BUMP_REVISION_SQL)) {
            ps.setString(1, capability);
            ps.setLong(2, now);
            ps.executeUpdate();
        }
    }

    private static String placeholders(int count) {
        StringBuilder sb = new StringBuilder(count * 2);
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append('?');
        }
        return sb.toString();
    }

    private static Placement mapRow(ResultSet rs) throws SQLException {
        return new Placement(
                rs.getInt("partition_id"),
                NodeId.of(rs.getString("node_id")),
                PlacementRole.valueOf(rs.getString("role")),
                rs.getLong("epoch"),
                rs.getLong("valid_after_ms"));
    }
}
