package com.ww.cache;

/**
 * XADD 命令的可选参数。
 *
 * <p>对应 Redis XADD 语法：
 * <pre>
 * XADD key [NOMKSTREAM] [MAXLEN [~|=] count | MINID [~|=] id] ID field value [field value ...]
 * </pre>
 *
 * <p>当前 P0 支持 MAXLEN 与 NOMKSTREAM；MINID 留待 P2 增强。
 */
public final class XAddOptions {

    /** 不自动创建 stream（若 stream 不存在则返回 null）。 */
    private final boolean nomkstream;
    /** MAXLEN 截断阈值；&lt;=0 表示不截断。 */
    private final long maxLen;
    /** ~ 近似截断（允许少量偏差以提升性能），否则精确截断。 */
    private final boolean approximate;

    private XAddOptions(Builder b) {
        this.nomkstream = b.nomkstream;
        this.maxLen = b.maxLen;
        this.approximate = b.approximate;
    }

    public boolean isNomkstream() {
        return nomkstream;
    }

    public long getMaxLen() {
        return maxLen;
    }

    public boolean isApproximate() {
        return approximate;
    }

    public boolean hasTrim() {
        return maxLen > 0;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static XAddOptions defaults() {
        return new Builder().build();
    }

    public static final class Builder {
        private boolean nomkstream = false;
        private long maxLen = -1L;
        private boolean approximate = false;

        public Builder nomkstream(boolean v) { this.nomkstream = v; return this; }
        public Builder maxLen(long v) { this.maxLen = v; return this; }
        public Builder approximate(boolean v) { this.approximate = v; return this; }

        public XAddOptions build() {
            return new XAddOptions(this);
        }
    }
}
