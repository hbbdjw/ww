package com.ww.runtime.lb;

/**
 * LB 配置存储：集群单行配置的读取与 CAS 更新。
 *
 * <p>实现必须线程安全；{@link #get()} 在缺行时负责播种默认配置（幂等）。</p>
 */
public interface LbConfigStore {

    /** 读取当前配置（缺行时播种并返回 {@link LbConfig#defaultConfig()}）。 */
    LbConfig get();

    /**
     * 基于版本号的乐观锁更新：仅当库中当前 version 等于 {@code expectedVersion} 时写入，
     * 成功后 version+1 并返回最新配置；版本不符抛 {@link LbConfigConflictException}。
     *
     * @param next             期望写入的配置内容（其 version 字段以 expectedVersion 为准）
     * @param expectedVersion  调用方读取到的版本号
     */
    LbConfig update(LbConfig next, long expectedVersion);
}
