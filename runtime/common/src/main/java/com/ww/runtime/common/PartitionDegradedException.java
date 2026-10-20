package com.ww.runtime.common;

/**
 * 分区隔离（DEGRADED）：该分区唯一持有者已死亡且无存活副本，数据面流量被主动拒绝。
 *
 * <p>隔离语义（替代此前的「静默空分区迁移」）：放置表保持指向死亡节点——
 * 数据仍在其磁盘上（押注重启回归即自动恢复），但治愈宽限期赌输后不再以
 * 空分区覆盖放置表造成静默永久丢失；读写一律快速失败（503），直至属主
 * 回归或运营显式 {@code force-adopt}（接受丢失）。</p>
 */
public class PartitionDegradedException extends RuntimeFrameworkException {

    private final String capability;
    private final int partition;
    private final String owner;

    public PartitionDegradedException(String capability, int partition, String owner) {
        super("partition " + capability + "/" + partition + " is quarantined (DEGRADED): "
                + "owner " + owner + " dead with no live replica; data preserved on owner's disk, "
                + "reads/writes blocked until owner rejoins or operator force-adopts");
        this.capability = capability;
        this.partition = partition;
        this.owner = owner;
    }

    public String capability() {
        return capability;
    }

    public int partition() {
        return partition;
    }

    public String owner() {
        return owner;
    }
}
