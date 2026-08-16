package com.ww.runtime.server.migration;

import com.ww.runtime.migration.MigrationEngine;
import com.ww.runtime.placement.RebalancePlanner.PartitionMove;
import com.ww.runtime.scheduler.MigrationCoordinator;

import java.util.List;
import java.util.Objects;

/**
 * {@link MigrationCoordinator} 的宿主适配：把 scheduler 模块的迁移协调端口
 * 委托给 migration 模块的 {@link MigrationEngine#migrateAndWait}。
 *
 * <p>失败透传为 {@code false}（engine 内部把失败任务置 FAILED 后返回 false，
 * 不抛异常），调度器保留节点现状并在下个周期重算重试。</p>
 */
public final class RuntimeMigrationCoordinator implements MigrationCoordinator {

    private final MigrationEngine engine;

    public RuntimeMigrationCoordinator(MigrationEngine engine) {
        this.engine = Objects.requireNonNull(engine, "engine");
    }

    @Override
    public boolean migrateAndWait(String capability, List<PartitionMove> moves) {
        return engine.migrateAndWait(capability, moves);
    }
}
