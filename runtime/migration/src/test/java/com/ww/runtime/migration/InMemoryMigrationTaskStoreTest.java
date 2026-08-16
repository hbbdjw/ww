package com.ww.runtime.migration;

import com.ww.runtime.common.IllegalStateTransitionException;
import com.ww.runtime.common.MigrationException;
import com.ww.runtime.common.MigrationTaskState;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.TestClock;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * InMemoryMigrationTaskStore：create / transition（CAS + 状态机）/ saveProgress / 列表口径。
 */
class InMemoryMigrationTaskStoreTest {

    private final TestClock clock = new TestClock(1_000L);
    private final InMemoryMigrationTaskStore store = new InMemoryMigrationTaskStore(clock);

    @Test
    void createAssignsSequentialIdsAndInitialPending() {
        long id1 = store.create("mock", 3, NodeId.of("node-a"), NodeId.of("node-b"));
        long id2 = store.create("mock", 4, null, NodeId.of("node-b"));
        assertThat(id1).isEqualTo(1L);
        assertThat(id2).isEqualTo(2L);

        MigrationTask t1 = store.get(id1).orElseThrow();
        assertThat(t1.capability()).isEqualTo("mock");
        assertThat(t1.partition()).isEqualTo(3);
        assertThat(t1.fromNode()).isEqualTo(NodeId.of("node-a"));
        assertThat(t1.toNode()).isEqualTo(NodeId.of("node-b"));
        assertThat(t1.state()).isEqualTo(MigrationTaskState.PENDING);
        assertThat(t1.checkpointOffset()).isZero();
        assertThat(t1.itemsDone()).isZero();
        assertThat(t1.checkpointJson()).isNull();
        assertThat(t1.updatedAtMillis()).isEqualTo(1_000L);

        MigrationTask t2 = store.get(id2).orElseThrow();
        assertThat(t2.fromNode()).isNull(); // 新增分区无历史归属
    }

    @Test
    void getAbsentReturnsEmpty() {
        assertThat(store.get(99L)).isEmpty();
    }

    @Test
    void legalTransitionsAdvanceAndUpdateTimestamp() {
        long id = store.create("mock", 0, NodeId.of("a"), NodeId.of("b"));
        MigrationTaskState[] happyPath = {
                MigrationTaskState.PREPARING, MigrationTaskState.SNAPSHOT, MigrationTaskState.COPYING,
                MigrationTaskState.SYNCING, MigrationTaskState.CUTOVER, MigrationTaskState.VERIFYING,
                MigrationTaskState.COMPLETED};
        for (MigrationTaskState next : happyPath) {
            clock.advance(10L);
            assertThat(store.transition(id, next)).isTrue();
            assertThat(store.get(id).orElseThrow().state()).isEqualTo(next);
        }
        assertThat(store.get(id).orElseThrow().updatedAtMillis()).isEqualTo(1_000L + 10L * happyPath.length);
    }

    @Test
    void illegalTransitionThrowsAndStateUnchanged() {
        long id = store.create("mock", 0, NodeId.of("a"), NodeId.of("b"));
        assertThatThrownBy(() -> store.transition(id, MigrationTaskState.COMPLETED))
                .isInstanceOf(IllegalStateTransitionException.class);
        assertThat(store.get(id).orElseThrow().state()).isEqualTo(MigrationTaskState.PENDING);

        store.transition(id, MigrationTaskState.PREPARING);
        assertThatThrownBy(() -> store.transition(id, MigrationTaskState.PENDING)) // 无回退边
                .isInstanceOf(IllegalStateTransitionException.class);
        assertThatThrownBy(() -> store.transition(id, MigrationTaskState.PREPARING)) // 同态自跃迁
                .isInstanceOf(IllegalStateTransitionException.class);
        assertThat(store.get(id).orElseThrow().state()).isEqualTo(MigrationTaskState.PREPARING);
    }

    @Test
    void transitionUnknownTaskThrows() {
        assertThatThrownBy(() -> store.transition(42L, MigrationTaskState.PREPARING))
                .isInstanceOf(MigrationException.class)
                .hasMessageContaining("42");
    }

    @Test
    void cancelledCannotGoFailed() {
        long id = store.create("mock", 0, NodeId.of("a"), NodeId.of("b"));
        store.transition(id, MigrationTaskState.CANCELLED);
        assertThatThrownBy(() -> store.transition(id, MigrationTaskState.FAILED))
                .isInstanceOf(IllegalStateTransitionException.class);
        assertThat(store.get(id).orElseThrow().state()).isEqualTo(MigrationTaskState.CANCELLED);
    }

    @Test
    void saveProgressUpdatesProgressButKeepsStateAndJsonWhenNull() {
        long id = store.create("mock", 0, NodeId.of("a"), NodeId.of("b"));
        walkTo(id, MigrationTaskState.COPYING);
        clock.advance(5L);
        store.saveProgress(id, 120L, 120L, "{\"epoch\":0,\"sourceCount\":500}");

        MigrationTask task = store.get(id).orElseThrow();
        assertThat(task.checkpointOffset()).isEqualTo(120L);
        assertThat(task.itemsDone()).isEqualTo(120L);
        assertThat(task.checkpointJson()).isEqualTo("{\"epoch\":0,\"sourceCount\":500}");
        assertThat(task.state()).isEqualTo(MigrationTaskState.COPYING); // 进度不改状态
        assertThat(task.updatedAtMillis()).isEqualTo(1_005L);

        clock.advance(5L);
        store.saveProgress(id, 240L, 240L, null); // json=null 保留原值
        MigrationTask again = store.get(id).orElseThrow();
        assertThat(again.checkpointOffset()).isEqualTo(240L);
        assertThat(again.checkpointJson()).isEqualTo("{\"epoch\":0,\"sourceCount\":500}");
        assertThat(again.updatedAtMillis()).isEqualTo(1_010L);

        assertThatThrownBy(() -> store.saveProgress(7L, 1L, 1L, null))
                .isInstanceOf(MigrationException.class);
    }

    @Test
    void listActiveExcludesTerminalAndListAllIncludesEverything() {
        long id1 = store.create("mock", 0, NodeId.of("a"), NodeId.of("b"));
        long id2 = store.create("mock", 1, NodeId.of("a"), NodeId.of("b"));
        long id3 = store.create("mock", 2, NodeId.of("a"), NodeId.of("b"));
        walkTo(id2, MigrationTaskState.COMPLETED);
        store.transition(id3, MigrationTaskState.CANCELLED);

        List<MigrationTask> active = store.listActive();
        assertThat(active).hasSize(1);
        assertThat(active.get(0).taskId()).isEqualTo(id1);

        List<MigrationTask> all = store.listAll();
        assertThat(all).hasSize(3);
        assertThat(all).extracting(MigrationTask::taskId).containsExactly(id1, id2, id3);
    }

    /** 沿合法路径推进到目标状态（PENDING 起步）。 */
    private void walkTo(long id, MigrationTaskState target) {
        MigrationTaskState current = store.get(id).orElseThrow().state();
        MigrationTaskState[] path = {
                MigrationTaskState.PREPARING, MigrationTaskState.SNAPSHOT, MigrationTaskState.COPYING,
                MigrationTaskState.SYNCING, MigrationTaskState.CUTOVER, MigrationTaskState.VERIFYING,
                MigrationTaskState.COMPLETED};
        for (MigrationTaskState next : path) {
            if (current == target) {
                return;
            }
            store.transition(id, next);
            current = next;
        }
    }
}
