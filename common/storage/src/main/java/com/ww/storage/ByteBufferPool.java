package com.ww.storage;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 直接内存 {@link ByteBuffer} 池：避免每次 IO 分配堆外内存的开销，并降低 GC 压力。
 *
 * <p>设计要点：
 * <ul>
 *   <li>固定大小缓冲区（默认 64KB），用 {@code allocateDirect} 分配在堆外</li>
 *   <li>无锁回收队列 {@link ConcurrentLinkedDeque}，borrow/return 均为 O(1)</li>
 *   <li>池上限 {@code maxPoolSize} 防止无限增长；超出时归还即释放（由 Cleaner 回收）</li>
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

    private final int bufferSize;
    private final int maxPoolSize;
    private final ConcurrentLinkedDeque<ByteBuffer> pool = new ConcurrentLinkedDeque<>();
    private final AtomicLong totalAllocated = new AtomicLong();

    public ByteBufferPool(int bufferSize, int maxPoolSize) {
        if (bufferSize <= 0) throw new IllegalArgumentException("bufferSize 必须 > 0");
        if (maxPoolSize <= 0) throw new IllegalArgumentException("maxPoolSize 必须 > 0");
        this.bufferSize = bufferSize;
        this.maxPoolSize = maxPoolSize;
    }

    public int getBufferSize() { return bufferSize; }

    public int getMaxPoolSize() { return maxPoolSize; }

    public int getPoolSize() { return pool.size(); }

    public long getTotalAllocated() { return totalAllocated.get(); }

    /** 借出一个直接内存缓冲区（已 clear，可直接写入） */
    public LeasedBuffer borrow() {
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
        if (pool.size() < maxPoolSize) {
            pool.offer(buf);
        } else {
            // 池满，放弃引用由 Cleaner 回收
            totalAllocated.decrementAndGet();
        }
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
