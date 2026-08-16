package com.ww.runtime.scheduler;

import com.ww.runtime.placement.PlacementStore;
import com.ww.runtime.placement.RebalancePlanner.PartitionMove;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 测试用迁移协调器：记录 moves，按 CAS 逐分区 cutover 后返回 true
 * （模拟真实协调器「搬数据 + 切主」职责）。
 */
final class CutoverMigrationCoordinator implements MigrationCoordinator {

    final List<PartitionMove> recorded = new CopyOnWriteArrayList<>();
    private final PlacementStore placements;

    CutoverMigrationCoordinator(PlacementStore placements) {
        this.placements = placements;
    }

    @Override
    public boolean migrateAndWait(String capability, List<PartitionMove> moves) {
        recorded.addAll(moves);
        for (PartitionMove move : moves) {
            placements.get(capability, move.partition()).ifPresent(current ->
                    placements.cutover(capability, move.partition(), move.to(), current.epoch()));
        }
        return true;
    }
}
