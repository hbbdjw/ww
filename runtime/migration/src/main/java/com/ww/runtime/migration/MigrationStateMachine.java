package com.ww.runtime.migration;

import com.ww.runtime.common.IllegalStateTransitionException;
import com.ww.runtime.common.MigrationTaskState;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * 迁移任务状态机：集中定义全部合法跃迁，任何状态写入前必须先经 {@link #check} 校验。
 *
 * <p>合法跃迁表：</p>
 * <pre>
 * PENDING   → PREPARING, CANCELLED
 * PREPARING → SNAPSHOT, FAILED, CANCELLED
 * SNAPSHOT  → COPYING, FAILED, CANCELLED
 * COPYING   → SYNCING, FAILED, CANCELLED
 * SYNCING   → CUTOVER, FAILED, CANCELLED
 * CUTOVER   → VERIFYING, FAILED          （切主已生效，不再允许取消）
 * VERIFYING → COMPLETED, FAILED
 * COMPLETED / FAILED / CANCELLED         （终态）
 * FAILED   → PENDING                     （唯一出边：允许重试）
 * </pre>
 *
 * <p>注意：终态中只有 FAILED 可跃迁（回 PENDING 重试）；同态自跃迁一律非法。</p>
 */
public final class MigrationStateMachine {

    private static final Map<MigrationTaskState, Set<MigrationTaskState>> TRANSITIONS = Map.of(
            MigrationTaskState.PENDING, EnumSet.of(MigrationTaskState.PREPARING, MigrationTaskState.CANCELLED),
            MigrationTaskState.PREPARING, EnumSet.of(MigrationTaskState.SNAPSHOT, MigrationTaskState.FAILED,
                    MigrationTaskState.CANCELLED),
            MigrationTaskState.SNAPSHOT, EnumSet.of(MigrationTaskState.COPYING, MigrationTaskState.FAILED,
                    MigrationTaskState.CANCELLED),
            MigrationTaskState.COPYING, EnumSet.of(MigrationTaskState.SYNCING, MigrationTaskState.FAILED,
                    MigrationTaskState.CANCELLED),
            MigrationTaskState.SYNCING, EnumSet.of(MigrationTaskState.CUTOVER, MigrationTaskState.FAILED,
                    MigrationTaskState.CANCELLED),
            MigrationTaskState.CUTOVER, EnumSet.of(MigrationTaskState.VERIFYING, MigrationTaskState.FAILED),
            MigrationTaskState.VERIFYING, EnumSet.of(MigrationTaskState.COMPLETED, MigrationTaskState.FAILED),
            MigrationTaskState.COMPLETED, EnumSet.noneOf(MigrationTaskState.class),
            MigrationTaskState.FAILED, EnumSet.of(MigrationTaskState.PENDING),
            MigrationTaskState.CANCELLED, EnumSet.noneOf(MigrationTaskState.class));

    private MigrationStateMachine() {
    }

    /**
     * 校验 from → to 是否合法。
     *
     * @throws IllegalStateTransitionException 非法跃迁
     */
    public static void check(MigrationTaskState from, MigrationTaskState to) {
        if (from == null || to == null) {
            throw new IllegalStateTransitionException(from, to, "state must not be null");
        }
        if (!TRANSITIONS.get(from).contains(to)) {
            throw new IllegalStateTransitionException(from, to);
        }
    }

    /** 是否合法跃迁（不抛异常版本，供探测用）。 */
    public static boolean canTransition(MigrationTaskState from, MigrationTaskState to) {
        return from != null && to != null && TRANSITIONS.get(from).contains(to);
    }

    /** 是否终态（COMPLETED / FAILED / CANCELLED）。 */
    public static boolean isTerminal(MigrationTaskState state) {
        return state == MigrationTaskState.COMPLETED
                || state == MigrationTaskState.FAILED
                || state == MigrationTaskState.CANCELLED;
    }
}
