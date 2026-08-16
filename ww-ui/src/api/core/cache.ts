import { requestClient } from '#/api/request';

/** 节点本地统计信息（JMX 快照 + 本地缓存视图） */
export interface CacheNodeStats {
  /** 本地缓存 key 数量 */
  keyCount: number;
  /** 进程 CPU 负载（0-1） */
  processCpuLoad: number;
  /** 系统 CPU 负载（0-1） */
  systemCpuLoad: number;
  /** 堆内存已用（MB） */
  heapUsedMb: number;
  /** 堆内存上限（MB） */
  heapMaxMb: number;
  /** 堆内存使用率（%） */
  heapUsedPercent: number;
  /** JVM 运行时长（秒） */
  uptimeSec: number;
  /** 线程数 */
  threadCount: number;
}

/** 缓存节点视图 */
export interface CacheNode {
  nodeKey: string;
  host: string;
  port: number;
  weight: number;
  /** 是否为本节点 */
  self: boolean;
  /** ALIVE / SUSPECT / DEAD */
  status: string;
  /** 是否在线（实时探测） */
  online: boolean;
  stats: CacheNodeStats | null;
  error?: string;
}

/** 缓存节点总览 */
export interface CacheNodesOverview {
  timestamp: number;
  topologyVersion: number;
  selfNodeKey: string;
  nodes: CacheNode[];
  onlineCount: number;
  totalKeyCount: number;
}

/** key 条目（附带持有节点） */
export interface CacheKeyEntry {
  key: string;
  nodeKey: string;
}

/** key 列表结果 */
export interface CacheKeysResult {
  keys: CacheKeyEntry[];
  total: number;
  truncated: boolean;
}

/** key 的值与 TTL 信息 */
export interface CacheValueInfo {
  exists: boolean;
  key: string;
  value: any;
  /** -1 不存在 / Long.MAX_VALUE(9007199254740991 精度内) 永不过期 / >=0 剩余毫秒 */
  ttlMillis: number;
  nodeKey: string | null;
}

/**
 * 获取缓存节点总览（节点列表 + 在线状态 + 每节点统计）
 */
export async function getCacheNodesApi(): Promise<CacheNodesOverview> {
  return requestClient.post<CacheNodesOverview>('/cache/admin/nodes');
}

/**
 * 获取 key 列表（按节点聚合，支持关键字过滤）
 * @param nodeKey 节点标识（可选，为空聚合全部节点）
 * @param keyword 过滤关键字（可选，大小写不敏感包含匹配）
 * @param limit 返回上限，默认 2000
 */
export async function getCacheKeysApi(params: {
  keyword?: string;
  limit?: number;
  nodeKey?: string;
}): Promise<CacheKeysResult> {
  return requestClient.post<CacheKeysResult>('/cache/admin/keys', params);
}

/**
 * 读取 key 的值与剩余 TTL
 * @param key 缓存键
 * @param nodeKey 持有节点（可选，优先在指定节点查找）
 */
export async function getCacheValueApi(
  key: string,
  nodeKey?: string,
): Promise<CacheValueInfo> {
  return requestClient.post<CacheValueInfo>('/cache/admin/value', {
    key,
    nodeKey: nodeKey || undefined,
  });
}

/**
 * 新增/修改 key-value
 * @param key 缓存键
 * @param value 值（JSON 文本或普通字符串）
 * @param ttlMillis 过期时间（毫秒），<=0 表示永不过期
 */
export async function saveCacheValueApi(
  key: string,
  value: string,
  ttlMillis = 0,
) {
  return requestClient.post('/cache/admin/save', { key, ttlMillis, value });
}

/**
 * 删除 key
 */
export async function deleteCacheKeyApi(key: string) {
  return requestClient.post('/cache/admin/delete', { key });
}

/** 超过此阈值视为永不过期（后端 Long.MAX_VALUE 经 JSON 传输后的数量级判定） */
export const TTL_NEVER_EXPIRES = 1e18;

/**
 * 格式化 TTL 为可读文本
 * @param ttlMillis -1 不存在 / Long.MAX_VALUE 永不过期 / >=0 剩余毫秒
 */
export function formatTtl(ttlMillis: number): string {
  if (ttlMillis < 0) return '不存在';
  if (ttlMillis >= TTL_NEVER_EXPIRES) return '永不过期';
  if (ttlMillis < 1000) return `${ttlMillis}ms`;
  const sec = Math.floor(ttlMillis / 1000);
  if (sec < 60) return `${sec}s`;
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min}m${sec % 60}s`;
  const h = Math.floor(min / 60);
  if (h < 24) return `${h}h${min % 60}m`;
  return `${Math.floor(h / 24)}d${h % 24}h`;
}
