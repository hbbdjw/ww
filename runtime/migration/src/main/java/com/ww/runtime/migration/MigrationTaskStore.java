package com.ww.runtime.migration;

import com.ww.runtime.common.MigrationTaskState;
import com.ww.runtime.common.NodeId;

import java.util.List;
import java.util.Optional;

/**
 * 迁移任务存储端口：任务元数据 + 状态 + 断点进度的持久化。
 *
 * <p>实现必须保证 {@link #transition} 的原子性：先读当前状态、经
 * {@link MigrationStateMachine#check} 校验，再以
 * {@code UPDATE ... WHERE state = <读到的状态>} 的 CAS 方式写入；
 * 任意一步失败（任务不存在 / 非法跃迁 / CAS 冲突）抛运行时异常且不落任何写入。</p>
 */
public interface MigrationTaskStore {

    /**
     * 创建任务（初始 PENDING，id 自增分配）。
     *
     * @return 新任务 id
     */
    long create(String capability, int partition, NodeId from, NodeId to);

    /** 读取任务；不存在返回 empty。 */
    Optional<MigrationTask> get(long taskId);

    /** 全部非终态（PENDING..VERIFYING）任务，按 taskId 升序。 */
    List<MigrationTask> listActive();

    /** 全部任务（含终态），按 taskId 升序。 */
    List<MigrationTask> listAll();

    /**
     * 状态跃迁（状态机校验 + CAS 写）。
     *
     * @return 恒 true（失败路径直接抛异常）
     * @throws com.ww.runtime.common.IllegalStateTransitionException 非法跃迁
     * @throws com.ww.runtime.common.MigrationException              任务不存在或 CAS 并发冲突
     */
    boolean transition(long taskId, MigrationTaskState newState);

    /**
     * 持久化断点进度（不改状态）。
     *
     * @param checkpointJson 断点元数据；传 null 表示保留原值
     */
    void saveProgress(long taskId, long checkpointOffset, long itemsDone, String checkpointJson);
}
