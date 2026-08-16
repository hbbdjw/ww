package com.ww.runtime.migration;

import com.ww.runtime.common.Clock;
import com.ww.runtime.common.MigrationException;
import com.ww.runtime.common.MigrationTaskState;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.SystemClock;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 进程内 {@link MigrationTaskStore} 实现：AtomicLong 分配 id，
 * {@link ConcurrentHashMap} 保存任务；transition/saveProgress 在 synchronized
 * 块内完成「读 → 校验 → 写」，线程安全。
 */
public final class InMemoryMigrationTaskStore implements MigrationTaskStore {

    private final Clock clock;
    private final AtomicLong idSeq = new AtomicLong();
    private final Map<Long, MigrationTask> tasks = new ConcurrentHashMap<>();

    public InMemoryMigrationTaskStore() {
        this(SystemClock.instance());
    }

    public InMemoryMigrationTaskStore(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public long create(String capability, int partition, NodeId from, NodeId to) {
        if (capability == null || capability.isBlank()) {
            throw new IllegalArgumentException("capability must not be null or blank");
        }
        if (partition < 0) {
            throw new IllegalArgumentException("partition must be >= 0, got " + partition);
        }
        Objects.requireNonNull(to, "to");
        long id = idSeq.incrementAndGet();
        long now = clock.nowMillis();
        tasks.put(id, new MigrationTask(id, capability, partition, from, to,
                MigrationTaskState.PENDING, 0L, 0L, null, now));
        return id;
    }

    @Override
    public Optional<MigrationTask> get(long taskId) {
        return Optional.ofNullable(tasks.get(taskId));
    }

    @Override
    public List<MigrationTask> listActive() {
        return list(task -> !MigrationStateMachine.isTerminal(task.state()));
    }

    @Override
    public List<MigrationTask> listAll() {
        return list(task -> true);
    }

    @Override
    public synchronized boolean transition(long taskId, MigrationTaskState newState) {
        Objects.requireNonNull(newState, "newState");
        MigrationTask current = tasks.get(taskId);
        if (current == null) {
            throw new MigrationException("migration task not found: " + taskId);
        }
        MigrationStateMachine.check(current.state(), newState);
        MigrationTask updated = current.withState(newState, clock.nowMillis());
        if (!tasks.replace(taskId, current, updated)) {
            throw new MigrationException(
                    "transition CAS failed for task " + taskId + ": state changed concurrently");
        }
        return true;
    }

    @Override
    public synchronized void saveProgress(long taskId, long checkpointOffset, long itemsDone,
                                          String checkpointJson) {
        MigrationTask current = tasks.get(taskId);
        if (current == null) {
            throw new MigrationException("migration task not found: " + taskId);
        }
        MigrationTask updated = current.withProgress(checkpointOffset, itemsDone,
                checkpointJson, clock.nowMillis());
        if (!tasks.replace(taskId, current, updated)) {
            throw new MigrationException(
                    "saveProgress CAS failed for task " + taskId + ": state changed concurrently");
        }
    }

    private List<MigrationTask> list(java.util.function.Predicate<MigrationTask> filter) {
        List<MigrationTask> result = new ArrayList<>();
        for (MigrationTask task : tasks.values()) {
            if (filter.test(task)) {
                result.add(task);
            }
        }
        result.sort(Comparator.comparingLong(MigrationTask::taskId));
        return result;
    }
}
