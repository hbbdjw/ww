package com.ww.runtime.common;

/**
 * 迁移任务状态。
 */
public enum MigrationTaskState {
    PENDING,
    PREPARING,
    SNAPSHOT,
    COPYING,
    SYNCING,
    CUTOVER,
    VERIFYING,
    COMPLETED,
    FAILED,
    CANCELLED
}
