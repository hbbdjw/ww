package com.ww.runtime.data.storage.engine;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 直接内存 {@link ByteBuffer} 池：避免每次 IO 分配堆外内存的开销，并降低 GC 压力。
 *
 * <p>设计要点：
 * <ul>
 *   <li>固定大小缓冲区（默认 64KB），用 {@code allocateDirect} 分配在堆外</li>
 *   <li>无锁回收队列 {@link ConcurrentLinkedDeque}，borrow/return 均为 O(1)</li>
 *   <li><b>T16：在借上限</b>——{@code maxPoolSize} 是「在借 + 可复用」的总量硬上限
 *       （Semaphore 许可制）：旧实现池空时无界新分配，并发 IO 尖峰直接把堆外打满
 *       （直接内存不受堆监控，OOM 表现为进程级崩溃）。超额借出限时等待
 *       （背压传导至调用方），等待超时抛 {@link StorageException} 而非无界堆积</li>
 *   <li>借出 {@link LeasedBuffer} 为 {@link AutoCloseable}，try-with-resources 自动归还</li>
 *   <li>归还前 {@code clear()} 重置 position/limit，借出方直接写入</li>
 * </ul>
 *
 * <p>典型用法：
 * <pre>{@code
 * try (ByteBufferPool.LeasedBuffer lb = pool.borrow()) {
 *     ByteBuffer buf = lb.buffer();
 *     channel.read(buf);
 *     buf.flip();
 *     ...
 * }
 * }</pre>
 */
public class ByteBufferPool {

    /** T16：超额借出的最长等待（背压窗口）；超时按失败处理，调用方转 503/重试。 */
    private static final long DEFAULT_BORROW_WAIT_MS = 5_000L;

    private final int bufferSize;
    private final int maxPoolSize;
    private final long borrowWaitMillis;
    private final ConcurrentLinkedDeque<ByteBuffer> pool = new ConcurrentLinkedDeque<>();
    /** T16：许可 = 在借 + 池内空闲 总量约束（分配前必获取，归还时释放）。 */
    private final Semaphore permits;
    private final AtomicLong totalAllocated = new AtomicLong();
    /** T16：等待超时拒绝次数（观测口：持续 >0 说明并发 IO 超出池容量，需调大 bufferPoolMaxSize） */
    private final AtomicLong borrowTimeouts = new AtomicLong();

    public ByteBufferPool(int bufferSize, int maxPoolSize) {
        this(bufferSize, maxPoolSize, DEFAULT_BORROW_WAIT_MS);
    }

    /** 测试可注入更短的背压等待窗（生产用双参构造）。 */
    public ByteBufferPool(int bufferSize, int maxPoolSize, long borrowWaitMillis) {
        if (bufferSize <= 0) throw new IllegalArgumentException("bufferSize 必须 > 0");
        if (maxPoolSize <= 0) throw new IllegalArgumentException("maxPoolSize 必须 > 0");
        if (borrowWaitMillis <= 0) throw new IllegalArgumentException("borrowWaitMillis 必须 > 0");
        this.bufferSize = bufferSize;
        this.maxPoolSize = maxPoolSize;
        this.borrowWaitMillis = borrowWaitMillis;
        this.permits = new Semaphore(maxPoolSize);
    }

    public int getBufferSize() { return bufferSize; }

    public int getMaxPoolSize() { return maxPoolSize; }

    public int getPoolSize() { return pool.size(); }

    public long getTotalAllocated() { return totalAllocated.get(); }

    /** T16：借出超时拒绝次数（观测口）。 */
    public long getBorrowTimeouts() { return borrowTimeouts.get(); }

    /**
     * 借出一个直接内存缓冲区（已 clear，可直接写入）。
     *
     * @throws StorageException 池总额度耗尽且等待超时（T16 背压）
     */
    public LeasedBuffer borrow() {
        boolean acquired;
        try {
            acquired = permits.tryAcquire(borrowWaitMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StorageException("等待直接内存缓冲池许可被中断", e);
        }
        if (!acquired) {
            borrowTimeouts.incrementAndGet();
            throw new StorageException("直接内存缓冲池在借达到上限 " + maxPoolSize
                    + "（" + (maxPoolSize * (long) bufferSize / 1024 / 1024) + "MB），等待超时");
        }
        ByteBuffer buf = pool.poll();
        if (buf == null) {
            buf = ByteBuffer.allocateDirect(bufferSize);
            totalAllocated.incrementAndGet();
        } else {
            buf.clear();
        }
        return new LeasedBuffer(buf, this);
    }

    private void release(ByteBuffer buf) {
        buf.clear();
        pool.offer(buf); // 池内数量恒 ≤ maxPoolSize（许可总量约束），无需再判满
        permits.release();
    }

    /** 借出句柄，try-with-resources 自动归还 */
    public static final class LeasedBuffer implements AutoCloseable {
        private final ByteBuffer buffer;
        private final ByteBufferPool pool;
        private volatile boolean released;

        LeasedBuffer(ByteBuffer buffer, ByteBufferPool pool) {
            this.buffer = buffer;
            this.pool = pool;
        }

        public ByteBuffer buffer() { return buffer; }

        @Override
        public void close() {
            if (!released) {
                released = true;
                pool.release(buffer);
            }
        }
    }
}
