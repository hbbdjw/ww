package com.ww.runtime.scheduler;

import com.ww.runtime.placement.RebalancePlanner.PartitionMove;

import java.util.List;

/**
 * {@link MigrationCoordinator} 的无数据实现：无本地分区数据的能力（如纯计算型 MOCK）
 * 无需搬迁，直接返回 {@code true}。
 *
 * <p>注意：本实现<b>不执行</b> {@code PlacementStore.cutover}——含数据能力的宿主
 * 必须接入真实迁移引擎适配器（搬数据 + CAS 切主），否则放置表不会收敛到新属主。</p>
 */
public final class NoopMigrationCoordinator implements MigrationCoordinator {

    @Override
    public boolean migrateAndWait(String capability, List<PartitionMove> moves) {
        return true;
    }
}
