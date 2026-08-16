package com.ww.runtime.migration;

import java.util.List;

/**
 * 分区数据适配器 SPI（spec R6 核心）：迁移引擎与具体存储之间的防腐层。
 *
 * <p>引擎只面向本接口完成 SNAPSHOT/COPYING/SYNCING/VERIFYING 各阶段的数据面操作；
 * 同一接口可由本地实现（{@link MockPartitionAdapter}，同进程直连）、
 * 远端实现（{@link RemotePartitionAdapter}，经 gRPC 拉取）或未来 cache/storage
 * 的真实实现提供。行格式约定：{@code key + 0x01 + value + 0x02}（UTF-8），
 * 一个分片（byte[]）可含一至多条目。</p>
 */
public interface PartitionDataAdapter {

    /**
     * 指定分区的条目总数（SNAPSHOT 阶段取源端计数）。
     */
    long itemCount(String capability, int partition);

    /**
     * 导出指定分区自断点起的全部数据分片。
     *
     * @param fromCheckpoint 条目偏移：跳过前 N 条（0 = 全量）；断点续跑天然依赖该语义
     * @return 剩余数据的分片列表（无数据返回空列表）
     */
    List<byte[]> exportChunks(String capability, int partition, long fromCheckpoint);

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
}
