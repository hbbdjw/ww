import { get, put, del } from './request'

/** 缓存 key 条目（runtime server 返回格式） */
export interface CacheKeyEntry {
  key: string
  ttlMillis: number
}

/** GET /cache/keys 响应（集群级 fan-out 聚合，与节点无关） */
export interface CacheKeysResult {
  total: number
  keys: CacheKeyEntry[]
  truncated: boolean
}

/** 值类型：Map/List 结构化值为 json，其余为 text */
export type CacheValueType = 'json' | 'text'

/** GET /cache/{key} 响应 */
export interface CacheValueInfo {
  key: string
  value: any
  type: CacheValueType
}

/** DELETE /cache/keys?prefix= 响应 */
export interface CachePrefixDeleteResult {
  deleted: number
  failed: number
}

/** 节点信息（来自 /rt/nodes） */
export interface RuntimeNodeInfo {
  nodeId: string
  host: string
  grpcPort: number
  state: string
  stateEpoch: number
  weight: number
}

/** 获取集群缓存 key 列表（fan-out 聚合） */
export async function getCacheKeysApi(params: {
  prefix?: string
  limit?: number
}): Promise<CacheKeysResult> {
  return get<CacheKeysResult>('/cache/keys', {
    prefix: params.prefix || undefined,
    limit: params.limit || 2000
  })
}

/** 获取缓存值 */
export async function getCacheValueApi(key: string): Promise<CacheValueInfo> {
  return get<CacheValueInfo>(`/cache/${encodeURIComponent(key)}`)
}

/** 保存缓存值（ttl 毫秒，0=永不过期；type=json 时后端解析为结构化对象存储） */
export async function saveCacheValueApi(
  key: string,
  value: string,
  ttlMillis = 0,
  type: CacheValueType = 'text'
) {
  return put(`/cache/${encodeURIComponent(key)}`, value, {
    params: { ttl: ttlMillis, type }
  })
}

/** 删除缓存 key */
export async function deleteCacheKeyApi(key: string) {
  return del(`/cache/${encodeURIComponent(key)}`)
}

/** 子树删除：删除匹配前缀的全部 key */
export async function deleteCachePrefixApi(prefix: string): Promise<CachePrefixDeleteResult> {
  return del<CachePrefixDeleteResult>('/cache/keys', { prefix })
}

export const TTL_NEVER_EXPIRES = 0

export function formatTtl(ttlMillis: number): string {
  if (ttlMillis <= 0) return '永不过期'
  if (ttlMillis < 1000) return `${ttlMillis}ms`
  const sec = Math.floor(ttlMillis / 1000)
  if (sec < 60) return `${sec}s`
  const min = Math.floor(sec / 60)
  if (min < 60) return `${min}m${sec % 60}s`
  const h = Math.floor(min / 60)
  if (h < 24) return `${h}h${min % 60}m`
  return `${Math.floor(h / 24)}d${h % 24}h`
}
