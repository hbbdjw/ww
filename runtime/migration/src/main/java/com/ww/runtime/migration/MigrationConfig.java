package com.ww.runtime.migration;

/**
 * 迁移引擎配置。
 *
 * @param maxConcurrentTasks 最大并发迁移任务数（默认 2）
 * @param maxBytesPerSecond  简单限流的目标速率（字节/秒，默认 64MB）
 * @param chunkSize          分片大小（字节，默认 4MB；限流睡眠时长按每片该大小估算）
 * @param ackWindow          传输 ack 窗口（在途未确认分片数，默认 8）
 */
public record MigrationConfig(int maxConcurrentTasks,
                              long maxBytesPerSecond,
                              int chunkSize,
                              int ackWindow) {

    public static final int DEFAULT_MAX_CONCURRENT_TASKS = 2;
    public static final long DEFAULT_MAX_BYTES_PER_SECOND = 64L * 1024 * 1024;
    public static final int DEFAULT_CHUNK_SIZE = ChunkCodec.DEFAULT_CHUNK_SIZE;
    public static final int DEFAULT_ACK_WINDOW = 8;

    public MigrationConfig {
        validate(maxConcurrentTasks, maxBytesPerSecond, chunkSize, ackWindow);
    }

    /** 默认配置：2 并发 / 64MBps / 4MB 分片 / ack 窗口 8。 */
    public static MigrationConfig defaults() {
        return new MigrationConfig(DEFAULT_MAX_CONCURRENT_TASKS, DEFAULT_MAX_BYTES_PER_SECOND,
                DEFAULT_CHUNK_SIZE, DEFAULT_ACK_WINDOW);
    }

    /**
     * 校验工厂：任一参数非法抛 {@link IllegalArgumentException}。
     */
    public static MigrationConfig of(int maxConcurrentTasks, long maxBytesPerSecond,
                                     int chunkSize, int ackWindow) {
        return new MigrationConfig(maxConcurrentTasks, maxBytesPerSecond, chunkSize, ackWindow);
    }

    /** 简单限流：每发一片的睡眠毫秒数 = chunkSize / 速率（整数截断，可为 0）。 */
    public long throttleMillisPerChunk() {
        return chunkSize * 1000L / maxBytesPerSecond;
    }

    private static void validate(int maxConcurrentTasks, long maxBytesPerSecond,
                                 int chunkSize, int ackWindow) {
        if (maxConcurrentTasks <= 0) {
            throw new IllegalArgumentException("maxConcurrentTasks must be > 0, got " + maxConcurrentTasks);
        }
        if (maxBytesPerSecond <= 0) {
            throw new IllegalArgumentException("maxBytesPerSecond must be > 0, got " + maxBytesPerSecond);
        }
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("chunkSize must be > 0, got " + chunkSize);
        }
        if (ackWindow <= 0) {
            throw new IllegalArgumentException("ackWindow must be > 0, got " + ackWindow);
        }
    }
}
