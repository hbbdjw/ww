/**
 * 集群管理「统一节点表」合并逻辑（纯函数，便于单测）。
 *
 * 背景：缓存集群节点（overview / cacheNodes）以 `host:cachePort` 为 nodeKey，
 * 存储集群节点（topology / health）以 `host:httpPort` 为 nodeKey。两者标识同一物理节点
 * 但 key 格式不同，若直接按 nodeKey 合并会得到「一物理节点两行」的错误结果。
 *
 * 后端在存储节点数据中附带 `cacheNodeKey`（cache 侧 nodeKey），这里以
 * `cacheNodeKey ?? nodeKey` 作为合并键，把同一物理节点的缓存/存储视图合并为一行；
 * 存储专属操作所需的 `host:httpPort` 键保留在 `storageNodeKey` 字段。
 */

export interface UnifiedNode {
  nodeKey: string;
  /** 存储侧节点键（host:httpPort），用于存储迁移/合并等操作；无存储视图时为空 */
  storageNodeKey?: string;
  host: string;
  cachePort: number;
  httpPort: number;
  weight: number;
  self: boolean;
  /** 缓存 membership 状态：ALIVE/SUSPECT/DEAD */
  cacheStatus: string;
  /** TCP 在线探测结果 */
  online: boolean;
  latencyMs: number;
  metrics: Record<string, any> | null;
  /** 存储健康节点（可能缺失） */
  storageStatus: string;
  storageFailures: number;
  storageEnabled: boolean;
  cacheEnabled: boolean;
  epoch: number;
  lastHeartbeatMs: number;
  /** 该节点是否参与存储拓扑 */
  hasStorage: boolean;
  /** 该节点是否参与缓存集群（membership 包含） */
  hasCache: boolean;
}

/** overview / cacheNodes 数据源节点（缓存侧） */
export interface CacheNodeSource {
  nodeKey: string;
  host?: string;
  port?: number;
  weight?: number;
  self?: boolean;
  status?: string;
  online?: boolean;
  latencyMs?: number;
  metrics?: Record<string, any> | null;
  epoch?: number;
  lastHeartbeatMs?: number;
  storageEnabled?: boolean;
  cacheEnabled?: boolean;
}

/** topology / health 数据源节点（存储侧） */
export interface StorageNodeSource {
  nodeKey: string;
  /** cache 侧 nodeKey，后端附带的统一合并键 */
  cacheNodeKey?: string;
  host?: string;
  httpPort?: number;
  weight?: number;
  self?: boolean;
  status?: string;
  failures?: number;
  storageEnabled?: boolean;
  cacheEnabled?: boolean;
}

export interface MergeClusterNodesInput {
  overview?: CacheNodeSource[];
  cacheNodes?: CacheNodeSource[];
  storageTopology?: StorageNodeSource[];
  storageHealth?: StorageNodeSource[];
}

export function mergeClusterNodes(input: MergeClusterNodesInput): UnifiedNode[] {
  const cacheMap = new Map<string, CacheNodeSource>();
  for (const n of input.cacheNodes ?? []) cacheMap.set(n.nodeKey, n);

  // 存储侧按 cacheNodeKey ?? nodeKey 归并到缓存侧的合并键上
  const storageTopoMap = new Map<string, StorageNodeSource>();
  for (const n of input.storageTopology ?? []) {
    storageTopoMap.set(n.cacheNodeKey ?? n.nodeKey, n);
  }

  const storageHealthMap = new Map<string, StorageNodeSource>();
  for (const n of input.storageHealth ?? []) {
    storageHealthMap.set(n.cacheNodeKey ?? n.nodeKey, n);
  }

  const overviewMap = new Map<string, CacheNodeSource>();
  for (const n of input.overview ?? []) overviewMap.set(n.nodeKey, n);

  const keys = new Set<string>();
  for (const k of cacheMap.keys()) keys.add(k);
  for (const k of storageTopoMap.keys()) keys.add(k);
  for (const k of overviewMap.keys()) keys.add(k);

  const result: UnifiedNode[] = [];
  for (const key of keys) {
    const cache = cacheMap.get(key);
    const sto = storageTopoMap.get(key);
    const health = storageHealthMap.get(key);
    const ov = overviewMap.get(key);
    result.push({
      nodeKey: key,
      // 存储操作 API 需要 host:httpPort 形式的节点键
      storageNodeKey: sto?.nodeKey ?? health?.nodeKey ?? undefined,
      host: ov?.host ?? cache?.host ?? sto?.host ?? '',
      cachePort: cache?.port ?? ov?.port ?? 0,
      httpPort: sto?.httpPort ?? health?.httpPort ?? 0,
      weight: ov?.weight ?? sto?.weight ?? cache?.weight ?? 1,
      self: ov?.self ?? cache?.self ?? sto?.self ?? false,
      cacheStatus: ov?.status ?? (cache ? (cache.online ? 'ALIVE' : 'DEAD') : '-'),
      online: cache?.online ?? false,
      latencyMs: cache?.latencyMs ?? -1,
      metrics: cache?.metrics ?? null,
      storageStatus: health?.status ?? '-',
      storageFailures: health?.failures ?? 0,
      storageEnabled: ov?.storageEnabled ?? sto?.storageEnabled ?? true,
      cacheEnabled: ov?.cacheEnabled ?? sto?.cacheEnabled ?? true,
      epoch: ov?.epoch ?? cache?.epoch ?? 0,
      lastHeartbeatMs: ov?.lastHeartbeatMs ?? cache?.lastHeartbeatMs ?? 0,
      hasStorage: !!sto,
      hasCache: !!cache || !!ov,
    });
  }
  // self 节点置顶，其余按 nodeKey 排序
  result.sort((a, b) =>
    a.self === b.self ? a.nodeKey.localeCompare(b.nodeKey) : a.self ? -1 : 1,
  );
  return result;
}
