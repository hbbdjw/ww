package com.ww.runtime.capability;

import com.ww.runtime.common.CapabilityType;

/**
 * 能力模块 SPI：本框架中一切"可被托管的生命周期单元"（缓存、对象存储、计算引擎……）
 * 都通过本接口接入 {@link CapabilityManager} 的状态机，由宿主统一驱动
 * 启停 / 预热 / 排水 / 迁移 / 冻结。
 *
 * <h2>生命周期与状态机的对应</h2>
 * <ul>
 *   <li>{@link #start()}：在 DISABLED→STARTING 跃迁后被调用，执行重量级初始化
 *       （打开文件、建连接、回放日志）。成功后进入 WARMING。</li>
 *   <li>{@link #warm()}：在 STARTING→WARMING 跃迁后被调用，执行预热（加载热数据、
 *       建立本地索引）。成功后进入 ACTIVE，开始对外服务。</li>
 *   <li>{@link #drain()}：在 ACTIVE→DRAINING 跃迁后被调用，进入维护态：停止接收新写、
 *       等待在途请求排空。排空完成后可迁移（MIGRATING）或停止（STOPPING），
 *       也可通过 cancelDrain 回到 ACTIVE。</li>
 *   <li>{@link #stop()}：在 STOPPING 态被调用，释放全部资源。成功后回到 DISABLED。</li>
 *   <li>{@link #freeze(boolean)}：轻量维护开关，<b>不参与</b>状态机跃迁——冻结期间
 *       模块应拒绝新写（抛 {@code RuntimeFrameworkException("frozen")}）但保留读与存量数据。</li>
 * </ul>
 *
 * <h2>接入点示例</h2>
 *
 * <h3>1. 包装 common/cache 的 SimpleCache 为缓存能力</h3>
 * <pre>{@code
 * final class CacheCapabilityModule implements CapabilityModule {
 *     private final SimpleCache<String, byte[]> cache;
 *     private volatile boolean frozen;
 *
 *     public CapabilityType type() { return CapabilityType.MOCK; } // 后续替换为 CACHE
 *
 *     public void start() throws Exception {
 *         cache.initialize();          // 对应：初始化内存结构 + AOF 文件回放（崩溃恢复）
 *     }
 *     public void warm()   { cache.preloadHotKeys(); }  // 对应：预热热点 key
 *     public void drain()  { cache.stopAcceptingWrites(); } // 对应：维护态停新写
 *     public void stop()   { cache.flushAof(); cache.close(); }
 *     public void freeze(boolean frozen) {
 *         this.frozen = frozen;        // 对应：维护态停新写（配合 drain 或独立使用）
 *     }
 *     // migrate：由 MigrationEngine 驱动，配合 exportPartition/importPartition
 *     // 完成 DRAINING→MIGRATING→STOPPING 期间的 KV 数据搬迁（参见 MockCapability）。
 * }
 * }</pre>
 *
 * <h3>2. 包装对象存储引擎（原 common/storage，现 runtime-data 的 storage/engine）为对象存储能力</h3>
 * <pre>{@code
 * final class ObjectStoreCapabilityModule implements CapabilityModule {
 *     private final ObjectStore objects;
 *     private final BucketStore buckets;
 *
 *     public void start() throws Exception {
 *         buckets.mount();             // 对应：Drive 挂载（目录/桶就绪）
 *         objects.open();
 *     }
 *     public void warm()  { objects.warmMetadataCache(); }
 *     public void drain() { objects.stopAcceptingPuts(); } // 对应：维护态停新写
 *     public void stop()  { objects.close(); buckets.unmount(); }
 *     public void freeze(boolean frozen) { objects.setReadOnly(frozen); }
 *     // migrate：对应对象迁移——DRAINING 期间按桶/前缀枚举对象，
 *     // 经 MigrationEngine 传输到目标节点后删除本地副本（参见 MigrationTask）。
 * }
 * }</pre>
 *
 * 实现方注意：
 * <ul>
 *   <li>所有钩子在 {@link CapabilityManager} 命令线程上同步调用，实现应快速返回或自行异步化；</li>
 *   <li>钩子抛出的任何异常都会把能力置为 FAILED（markFailed）并包装为
 *       {@code RuntimeFrameworkException} 向上传播；</li>
 *   <li>实现必须容忍在 FAILED 后被重新 start（宿主会先经 stop 回到 DISABLED）。</li>
 * </ul>
 */
public interface CapabilityModule {

    /**
     * 本模块承载的能力类型（当前框架仅 {@link CapabilityType#MOCK}，预留扩展）。
     */
    CapabilityType type();

    /**
     * 初始化阶段钩子（DISABLED→STARTING 后调用）。
     * 打开底层资源、回放持久化日志；抛异常则能力进入 FAILED。
     */
    void start() throws Exception;

    /**
     * 预热阶段钩子（STARTING→WARMING 后调用）。
     * 加载热点数据，尽快达到可服务状态；抛异常则能力进入 FAILED。
     */
    void warm();

    /**
     * 排水钩子（ACTIVE→DRAINING 后调用）。
     * 停止接收新写、等待在途请求完成；抛异常则能力进入 FAILED。
     */
    void drain();

    /**
     * 停止钩子（STOPPING 态调用）。
     * 刷盘并释放全部资源；抛异常则能力进入 FAILED。
     */
    void stop();

    /**
     * 冻结/解冻开关（不参与状态机跃迁）。
     * 冻结后新写应抛 {@code RuntimeFrameworkException("frozen")}。
     */
    void freeze(boolean frozen);

    /**
     * 资源声明：调度器（scheduler 模块）用于装箱估算的静态权重，
     * 默认 {@link ResourceDeclaration#empty()}。
     */
    default ResourceDeclaration resources() {
        return ResourceDeclaration.empty();
    }

    /**
     * 资源声明值对象。
     *
     * @param cpuWeight   CPU 权重（非负，相对值，1.0 约等于一核）
     * @param memoryBytes 预留内存字节数（非负）
     * @param diskBytes   预留磁盘字节数（非负）
     */
    record ResourceDeclaration(double cpuWeight, long memoryBytes, long diskBytes) {

        public ResourceDeclaration {
            if (!Double.isFinite(cpuWeight) || cpuWeight < 0) {
                throw new IllegalArgumentException("cpuWeight must be >= 0, got " + cpuWeight);
            }
            if (memoryBytes < 0) {
                throw new IllegalArgumentException("memoryBytes must be >= 0, got " + memoryBytes);
            }
            if (diskBytes < 0) {
                throw new IllegalArgumentException("diskBytes must be >= 0, got " + diskBytes);
            }
        }

        /**
         * 空声明：无资源诉求（默认值）。
         */
        public static ResourceDeclaration empty() {
            return new ResourceDeclaration(0.0d, 0L, 0L);
        }
    }
}
