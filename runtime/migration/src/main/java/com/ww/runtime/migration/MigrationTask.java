package com.ww.runtime.migration;

import com.ww.runtime.common.MigrationTaskState;
import com.ww.runtime.common.NodeId;

import java.util.Objects;

/**
 * 一次分区迁移任务的不可变快照（读到的永远是 store 中某一时刻的值）。
 *
 * <p>{@code fromNode} 可为 null（目标集新增分区，无历史归属，与
 * {@code RebalancePlanner.PartitionMove.from} 口径一致）；其余字段均非空。</p>
 *
 * @param taskId           任务自增 id
 * @param capability       能力标识
 * @param partition        分区号
 * @param fromNode         源节点（可为 null）
 * @param toNode           目标节点
 * @param state            当前状态
 * @param checkpointOffset 已迁移条目偏移（重启续跑的断点）
 * @param itemsDone        已完成条目数
 * @param checkpointJson   断点元数据 JSON（引擎记录 epoch/sourceCount 等，可为 null）
 * @param updatedAtMillis  最后更新时间
 */
public record MigrationTask(long taskId,
                            String capability,
                            int partition,
                            NodeId fromNode,
                            NodeId toNode,
                            MigrationTaskState state,
                            long checkpointOffset,
                            long itemsDone,
                            String checkpointJson,
                            long updatedAtMillis) {

    public MigrationTask {
        if (capability == null || capability.isBlank()) {
            throw new IllegalArgumentException("capability must not be null or blank");
        }
        if (partition < 0) {
            throw new IllegalArgumentException("partition must be >= 0, got " + partition);
        }
        Objects.requireNonNull(toNode, "toNode");
        Objects.requireNonNull(state, "state");
        if (checkpointOffset < 0) {
            throw new IllegalArgumentException("checkpointOffset must be >= 0, got " + checkpointOffset);
        }
        if (itemsDone < 0) {
            throw new IllegalArgumentException("itemsDone must be >= 0, got " + itemsDone);
        }
    }

    /** 派生：仅变更状态与更新时间（进度字段保持）。 */
    public MigrationTask withState(MigrationTaskState newState, long newUpdatedAtMillis) {
        return new MigrationTask(taskId, capability, partition, fromNode, toNode, newState,
                checkpointOffset, itemsDone, checkpointJson, newUpdatedAtMillis);
    }

    /** 派生：仅变更进度字段与更新时间（checkpointJson 传 null 表示保留原值）。 */
    public MigrationTask withProgress(long newCheckpointOffset, long newItemsDone,
                                      String newCheckpointJson, long newUpdatedAtMillis) {
        return new MigrationTask(taskId, capability, partition, fromNode, toNode, state,
                newCheckpointOffset, newItemsDone,
                newCheckpointJson == null ? checkpointJson : newCheckpointJson,
                newUpdatedAtMillis);
    }
}
