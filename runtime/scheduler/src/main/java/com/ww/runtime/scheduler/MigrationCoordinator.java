package com.ww.runtime.scheduler;

import com.ww.runtime.placement.RebalancePlanner.PartitionMove;

import java.util.List;

/**
 * 迁移协调器端口：调度器与 migration 模块之间的解耦边界。
 *
 * <p>调度器只负责<b>规划</b>（{@code RebalancePlanner.plan}）与<b>驱动节点状态机</b>；
 * 分区数据的实际搬迁（源节点 export → 目标节点 import → 源端清理）与
 * <b>放置切主</b>均由本接口的实现完成。实现必须：</p>
 * <ol>
 *   <li><b>幂等</b>：同一批 moves 重复执行安全（调度器失败后下周期重试）；</li>
 *   <li><b>切主</b>：数据搬迁完成后，用 {@code PlacementStore.cutover(capability,
 *       partition, newNode, expectedEpoch)} 的 CAS 语义逐分区推进放置行，
 *       使放置表与新属主一致——cutover 职责完全在实现内，调度器不代劳；</li>
 *   <li><b>同步返回</b>：全部 moves 完成返回 {@code true}；任一失败返回
 *       {@code false}（调度器保留节点现状，下周期重算重试）。</li>
 * </ol>
 *
 * <p>宿主用真实 MigrationEngine 适配本接口；测试用 fake（记录 moves + 直接 cutover）。</p>
 */
public interface MigrationCoordinator {

    /**
     * 执行一批分区迁移并等待完成。
     *
     * @param capability 能力标识（CapabilityType.name()）
     * @param moves      迁移计划（可为空表：无数据可搬时应直接返回 true）
     * @return 是否全部成功
     */
    boolean migrateAndWait(String capability, List<PartitionMove> moves);
}
