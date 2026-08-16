package com.ww.runtime.membership;

/**
 * 成员关系模块配置：心跳周期与故障检测阈值。
 *
 * @param heartbeatIntervalMs 心跳间隔（毫秒），须为正
 * @param suspectAfterMisses 连续未收到消息多少个周期后将 peer 标记为 SUSPECTED
 * @param deadAfterMisses    连续未收到消息多少个周期后将 peer 标记为 DEAD
 */
public record MembershipConfig(long heartbeatIntervalMs, int suspectAfterMisses, int deadAfterMisses) {

    public static final long DEFAULT_HEARTBEAT_INTERVAL_MS = 1000L;
    public static final int DEFAULT_SUSPECT_AFTER_MISSES = 3;
    public static final int DEFAULT_DEAD_AFTER_MISSES = 6;

    public MembershipConfig {
        validate(heartbeatIntervalMs, suspectAfterMisses, deadAfterMisses);
    }

    public static MembershipConfig of(long heartbeatIntervalMs, int suspectAfterMisses, int deadAfterMisses) {
        return new MembershipConfig(heartbeatIntervalMs, suspectAfterMisses, deadAfterMisses);
    }

    public static MembershipConfig defaults() {
        return of(DEFAULT_HEARTBEAT_INTERVAL_MS, DEFAULT_SUSPECT_AFTER_MISSES, DEFAULT_DEAD_AFTER_MISSES);
    }

    /** SUSPECTED 判定阈值（毫秒）。 */
    public long suspectTimeoutMs() {
        return suspectAfterMisses * heartbeatIntervalMs;
    }

    /** DEAD 判定阈值（毫秒）。 */
    public long deadTimeoutMs() {
        return deadAfterMisses * heartbeatIntervalMs;
    }

    private static void validate(long heartbeatIntervalMs, int suspectAfterMisses, int deadAfterMisses) {
        if (heartbeatIntervalMs <= 0) {
            throw new IllegalArgumentException("heartbeatIntervalMs must be positive, got " + heartbeatIntervalMs);
        }
        if (suspectAfterMisses < 1) {
            throw new IllegalArgumentException("suspectAfterMisses must be >= 1, got " + suspectAfterMisses);
        }
        if (deadAfterMisses < suspectAfterMisses) {
            throw new IllegalArgumentException(
                    "deadAfterMisses must be >= suspectAfterMisses, got " + deadAfterMisses + " < " + suspectAfterMisses);
        }
    }
}
