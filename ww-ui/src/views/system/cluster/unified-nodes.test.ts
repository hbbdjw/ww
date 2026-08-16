import { describe, expect, it } from 'vitest';

import {
  mergeClusterNodes,
  type CacheNodeSource,
  type StorageNodeSource,
} from './unified-nodes';

/** 缓存侧节点（host:cachePort） */
function cacheNode(
  host: string,
  cachePort: number,
  extra: Partial<CacheNodeSource> = {},
): CacheNodeSource {
  return {
    nodeKey: `${host}:${cachePort}`,
    host,
    port: cachePort,
    weight: 1,
    self: false,
    status: 'ALIVE',
    online: true,
    latencyMs: 1,
    epoch: 0,
    lastHeartbeatMs: 0,
    storageEnabled: true,
    cacheEnabled: true,
    ...extra,
  };
}

/** 存储侧节点（host:httpPort），cacheNodeKey 由后端附带 */
function storageNode(
  host: string,
  httpPort: number,
  cacheNodeKey?: string,
  extra: Partial<StorageNodeSource> = {},
): StorageNodeSource {
  return {
    nodeKey: `${host}:${httpPort}`,
    cacheNodeKey,
    host,
    httpPort,
    weight: 1,
    self: false,
    storageEnabled: true,
    cacheEnabled: true,
    ...extra,
  };
}

describe('mergeClusterNodes 统一节点表合并', () => {
  it('同一物理节点的缓存/存储视图按 cacheNodeKey 合并为一行（3 节点 → 3 行）', () => {
    const rows = mergeClusterNodes({
      overview: [
        cacheNode('n1', 8701),
        cacheNode('n2', 8702),
        cacheNode('n3', 8703, { self: true }),
      ],
      cacheNodes: [
        cacheNode('n1', 8701),
        cacheNode('n2', 8702),
        cacheNode('n3', 8703, { self: true }),
      ],
      storageTopology: [
        storageNode('n1', 8081, 'n1:8701'),
        storageNode('n2', 8082, 'n2:8702'),
        storageNode('n3', 8083, 'n3:8703'),
      ],
      storageHealth: [
        storageNode('n1', 8081, 'n1:8701', { status: 'ALIVE', failures: 0 }),
        storageNode('n2', 8082, 'n2:8702', { status: 'DEAD', failures: 3 }),
        storageNode('n3', 8083, 'n3:8703', { status: 'ALIVE', failures: 0 }),
      ],
    });

    expect(rows).toHaveLength(3);
    expect(rows.map((r) => r.nodeKey).sort()).toEqual([
      'n1:8701',
      'n2:8702',
      'n3:8703',
    ]);
    for (const row of rows) {
      expect(row.hasCache).toBe(true);
      expect(row.hasStorage).toBe(true);
      expect(row.httpPort).toBeGreaterThan(0);
      // 存储操作键保持 host:httpPort 格式
      expect(row.storageNodeKey).toBe(row.host + ':' + row.httpPort);
    }
    // self 节点置顶
    expect(rows[0]?.nodeKey).toBe('n3:8703');
    // 存储健康信息并入同一行
    const n2 = rows.find((r) => r.nodeKey === 'n2:8702');
    expect(n2?.storageStatus).toBe('DEAD');
    expect(n2?.storageFailures).toBe(3);
  });

  it('存储节点无 cacheNodeKey 时退回自身 nodeKey（不合并，保持旧行为）', () => {
    const rows = mergeClusterNodes({
      cacheNodes: [cacheNode('n1', 8701), cacheNode('n2', 8702)],
      storageTopology: [storageNode('n1', 8081), storageNode('n2', 8082)],
    });

    expect(rows.map((r) => r.nodeKey).sort()).toEqual([
      'n1:8081',
      'n1:8701',
      'n2:8082',
      'n2:8702',
    ]);
  });

  it('纯存储节点（无缓存侧数据）单独成行', () => {
    const rows = mergeClusterNodes({
      cacheNodes: [cacheNode('n1', 8701)],
      storageTopology: [storageNode('n9', 8089, 'n9:8709')],
    });

    const onlyStorage = rows.find((r) => r.nodeKey === 'n9:8709');
    expect(onlyStorage).toBeDefined();
    expect(onlyStorage?.hasStorage).toBe(true);
    expect(onlyStorage?.hasCache).toBe(false);
    expect(onlyStorage?.cacheStatus).toBe('-');
    // 未匹配到 cache 节点时后端不会附带 cacheNodeKey，行键退回存储键
    const fallback = mergeClusterNodes({
      cacheNodes: [cacheNode('n1', 8701)],
      storageTopology: [storageNode('n9', 8089)],
    }).find((r) => r.host === 'n9');
    expect(fallback?.nodeKey).toBe('n9:8089');
  });
});
