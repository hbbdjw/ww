package com.ww.cache;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * AOF 持久化配置（Redis风格）
 */
@Component
@ConfigurationProperties(prefix = "cache.aof")
public class AofProperties {

    /** 是否启用 AOF，默认 false */
    private boolean enabled = false;

    /** fsync策略：always / everysec / no（对应Redis的appendfsync） */
    private String appendfsync = "everysec";

    /** AOF文件目录 */
    private String dir = "./aof";

    /** AOF文件名基础名 */
    private String filename = "appendonly.aof";

    /** 是否启用AOF自动重写 */
    private boolean autoRewrite = true;

    /** AOF重写触发：文件大小相比上次重写增长的最小百分比 */
    private int autoRewritePercentage = 100;

    /** AOF重写触发：最小文件大小（MB），小于此值不触发重写 */
    private long autoRewriteMinSizeMb = 64;

    /** 单次刷盘最大重试次数 */
    private int flushRetries = 3;

    /** 刷盘失败重试退避基数（毫秒） */
    private long flushRetryBackoffMs = 100;

    /** 写入缓冲区大小（字节） */
    private int bufferSize = 8192;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getAppendfsync() {
        return appendfsync;
    }

    public void setAppendfsync(String appendfsync) {
        this.appendfsync = appendfsync;
    }

    public String getDir() {
        return dir;
    }

    public void setDir(String dir) {
        this.dir = dir;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public boolean isAutoRewrite() {
        return autoRewrite;
    }

    public void setAutoRewrite(boolean autoRewrite) {
        this.autoRewrite = autoRewrite;
    }

    public int getAutoRewritePercentage() {
        return autoRewritePercentage;
    }

    public void setAutoRewritePercentage(int autoRewritePercentage) {
        this.autoRewritePercentage = autoRewritePercentage;
    }

    public long getAutoRewriteMinSizeMb() {
        return autoRewriteMinSizeMb;
    }

    public void setAutoRewriteMinSizeMb(long autoRewriteMinSizeMb) {
        this.autoRewriteMinSizeMb = autoRewriteMinSizeMb;
    }

    public int getFlushRetries() {
        return flushRetries;
    }

    public void setFlushRetries(int flushRetries) {
        this.flushRetries = flushRetries;
    }

    public long getFlushRetryBackoffMs() {
        return flushRetryBackoffMs;
    }

    public void setFlushRetryBackoffMs(long flushRetryBackoffMs) {
        this.flushRetryBackoffMs = flushRetryBackoffMs;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }
}
