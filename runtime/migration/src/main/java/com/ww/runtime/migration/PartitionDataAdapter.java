package com.ww.runtime.migration;

import com.ww.runtime.common.MigrationException;
import com.ww.runtime.common.NodeId;

import java.util.Iterator;
import java.util.List;
import java.util.function.IntPredicate;

/**
 * 分区数据适配器 SPI（spec R6 核心）：迁移引擎与具体存储之间的防腐层。
 *
 * <p>引擎只面向本接口完成 SNAPSHOT/COPYING/SYNCING/VERIFYING 各阶段的数据面操作；
 * 同一接口可由本地实现（{@link MockPartitionAdapter}，同进程直连）、
 * 远端实现（{@link RemotePartitionAdapter}，经 gRPC 拉取）、按任务源/目标路由的组合实现
 * （{@link RoutingPartitionAdapter}）或真实能力适配器（Cache/Storage）提供。
 * 行格式约定：{@code key + 0x01 + ... + 0x02}（UTF-8），一个分片（byte[]）可含一至多条目。</p>
 *
 * <h2>节点感知重载（跨节点迁移）</h2>
 * <p>带 {@code source}/{@code target} 的 default 重载由迁移引擎调用；本地适配器无需关心，
 * 默认委托同名本地方法。{@link RoutingPartitionAdapter} 按 source/target 把调用路由到
 * 「本地组合适配器 | 远端节点」——任何能力注册进组合适配器即自动获得跨节点迁移。</p>
 *
 * <h2>能力可选扩展（源端清理 / 启动清扫 / 持久化压实）</h2>
 * <ul>
 *   <li>{@link #clearPartition}：迁移完成后清理源端分区数据（引擎尽力调用，失败仅告警）；</li>
 *   <li>{@link #retainPartitions}：启动清扫——保留谓词通过的分区，删除其余（返回删除条目数）；</li>
 *   <li>{@link #compact}：持久化压实（如 AOF 重写），清扫后调用。</li>
 * </ul>
 * 未实现的能力走 default（抛 {@link MigrationException} / 返回 0 / no-op），引擎与清扫器
 * 均按「尽力而为」处理，不阻断调度。
 */
public interface PartitionDataAdapter {

    /**
     * 指定分区的条目总数（SNAPSHOT 阶段取源端计数）。
     */
    long itemCount(String capability, int partition);

    /**
     * 导出指定分区自断点起的全部数据分片（惰性流）。
     *
     * <p>返回<b>惰性迭代器</b>：每次 {@code next()} 至多产生一个分片，实现方应按需
     * 产出（磁盘逐对象读取 / 网络逐片拉取），不得先把整个分区物化入堆——
     * 大分区全量入堆是节点 OOM 的直接诱因。调用方边消费边推进断点，
     * 堆内占用恒定在单片量级。</p>
     *
     * @param fromCheckpoint 条目偏移：跳过前 N 条（0 = 全量）；断点续跑天然依赖该语义
     * @return 剩余数据的分片流（无数据返回空迭代器）
     */
    Iterator<byte[]> exportChunks(String capability, int partition, long fromCheckpoint);

    /**
     * 键型游标导出：除位置断点外，跳过所有 {@code <= cursor} 的条目。
     *
     * <p>位置断点在源端并发写入时会因字典序插入/删除而错位（已续跑过的对象
     * 被整体跳过、未迁移对象被漏掉）。键型游标不随集合长度变化，是并发迁移的
     * 安全断点。默认实现忽略游标（旧适配器/测试桩行为不变）。</p>
     *
     * @param cursor 不透明游标（由 {@link #cursorOf} 产出；null = 无游标，仅按位置断点导出）
     */
    default Iterator<byte[]> exportChunks(String capability, int partition, long fromCheckpoint,
                                          String cursor) {
        return exportChunks(capability, partition, fromCheckpoint);
    }

    /**
     * 从一个已成功导入的分片中提取“最后一条完整条目”的游标；分片不含完整条目
     * （如存储对象的中间片）返回 null。默认不支持游标。
     */
    default String cursorOf(String capability, byte[] piece) {
        return null;
    }

    /** 适配器是否支持键型游标（不支持时迁移引擎保持旧的位置断点语义）。 */
    default boolean supportsCursorResume() {
        return false;
    }

    /**
     * 分区写冻结（迁移 cutover 前停写）：{@code ttlMillis} 后自动解除，避免执行节点
     * 崩溃后分区永久不可写。默认 no-op（Mock/测试桩无需冻结）。
     *
     * @return 是否成功冻结（真实适配器失败应返回 false，由引擎中止迁移）
     */
    default boolean freezePartition(String capability, int partition, long ttlMillis) {
        return true;
    }

    /**
     * 关闭导出迭代器（T24）：迁移中止/异常/取消路径必须调用——实现方的惰性迭代器
     * 可能持有打开的文件句柄（本地导出）或网络流（远端导出），不关闭则 FD 泄漏。
     */
    default void closeExportChunks(Iterator<byte[]> iterator) {
        if (iterator instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception ignored) {
                // 关闭失败不影响迁移主流程
            }
        }
    }

    /**
     * 导入分片（幂等 upsert 语义：同 key 重复导入不产生副作用，也不产生重复键）。
     *
     * @return 成功导入返回 true；数据非法（解析失败/分区归属不符等）返回 false
     */
    boolean importChunks(String capability, int partition, List<byte[]> chunks);

    /**
     * 目标端可校验的条目计数（VERIFYING 阶段对账用；对 Mock 为精确值）。
     */
    long verifyCount(String capability, int partition);

    // ------------------------------------------------------------------
    // 节点感知重载（跨节点迁移；default 委托本地实现）
    // ------------------------------------------------------------------

    /**
     * 指定分区在<b>源节点</b>上的条目总数。
     *
     * @param source 源节点（null 表示无历史归属的新分区——按 0 条计）
     */
    default long itemCount(String capability, int partition, NodeId source) {
        return itemCount(capability, partition);
    }

    /**
     * 从<b>源节点</b>导出指定分区自断点起的全部数据分片（惰性流，语义同本地重载）。
     *
     * @param source 源节点（null 表示无历史归属——返回空迭代器）
     */
    default Iterator<byte[]> exportChunks(String capability, int partition, long fromCheckpoint,
                                          NodeId source) {
        return exportChunks(capability, partition, fromCheckpoint);
    }

    /** 键型游标 + 源节点的导出重载（跨节点迁移）。 */
    default Iterator<byte[]> exportChunks(String capability, int partition, long fromCheckpoint,
                                          String cursor, NodeId source) {
        return exportChunks(capability, partition, fromCheckpoint, cursor);
    }

    /**
     * 冻结<b>源节点</b>上的分区写入（迁移 cutover 前停写，TTL 自愈）。
     */
    default boolean freezePartition(String capability, int partition, NodeId source,
                                    long ttlMillis) {
        return freezePartition(capability, partition, ttlMillis);
    }

    /**
     * 向<b>目标节点</b>导入分片（幂等 upsert）。
     *
     * @param target  目标节点
     * @param taskId  迁移任务 id（远端推送通道用于接收端进度持久化；本地实现忽略）
     */
    default boolean importChunks(String capability, int partition, List<byte[]> chunks,
                                 NodeId target, long taskId) {
        return importChunks(capability, partition, chunks);
    }

    /**
     * 向<b>目标节点</b>导入分片（幂等 upsert），携带发送端预计算的 SHA-256 校验和
     * （下标与 {@code chunks} 一一对应）：传输层直接透传该值作为线上 checksum，
     * 避免逐片重复哈希（引擎每片仅计算一次，接收端 TransferStream 按此校验）。
     *
     * <p>不需要校验和的实现走 default 委托（忽略该参数）。</p>
     *
     * @param checksums 各分片的 SHA-256 hex（null / 越界 / 空白项由实现方自行补算）
     */
    default boolean importChunks(String capability, int partition, List<byte[]> chunks,
                                 NodeId target, long taskId, List<String> checksums) {
        return importChunks(capability, partition, chunks, target, taskId);
    }

    /**
     * <b>目标节点</b>上可校验的条目计数。
     */
    default long verifyCount(String capability, int partition, NodeId target) {
        return verifyCount(capability, partition);
    }

    // ------------------------------------------------------------------
    // 能力可选扩展（源端清理 / 启动清扫 / 持久化压实）
    // ------------------------------------------------------------------

    /**
     * 清理<b>源节点</b>上指定分区的全部数据（迁移 COMPLETED 后由引擎尽力调用）。
     *
     * <p>default 抛 {@link MigrationException}（能力未实现清理）——引擎按尽力而为捕获告警，
     * 残留数据由该能力下次启动清扫（若实现 {@link #retainPartitions}）兜底。</p>
     *
     * @param source 源节点（null 无意义，调用方不会传 null）
     * @return 是否清理成功
     */
    default boolean clearPartition(String capability, int partition, NodeId source) {
        throw new MigrationException("clearPartition not supported for capability: " + capability);
    }

    /**
     * 启动清扫：仅保留谓词 {@code keep} 通过的分区，删除其余分区数据。
     *
     * <p>default 返回 0（能力未实现清扫——保留现状，不做任何删除）。</p>
     *
     * @return 删除的条目数（0 表示无删除或不支持）
     */
    default long retainPartitions(String capability, IntPredicate keep) {
        return 0L;
    }

    /**
     * 持久化压实（如 AOF 后台重写）；不支持的能力 no-op。
     */
    default void compact(String capability) {
        // no-op
    }
}
