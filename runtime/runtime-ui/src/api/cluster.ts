import { get, post, put, requestSSE } from './request'

/** 节点信息 */
export interface RuntimeNodeInfo {
  nodeId: string
  host: string
  grpcPort: number
  /** REST 数据面端口（负载均衡反代寻址；0=未上报） */
  httpPort?: number
  state: string
  stateEpoch: number
  weight: number
  /** 本节点 φ 失败检测器对该 peer 的当前怀疑度（/rt/status members 专有，本地视角） */
  phi?: number
}

/** 能力信息 */
export interface CapabilityInfo {
  nodeId: string
  type: string
  desired: string
  actual: string
  updatedAtMillis: number
}

/** GET /rt/status 响应 */
export interface RuntimeStatus {
  nodeId: string
  leader?: string | null
  isLeader?: boolean
  leaseOwner?: string | null
  capabilities: CapabilityInfo[]
  primariesByCapability?: Record<string, Record<string, string>>
  self?: RuntimeNodeInfo | null
  members: RuntimeNodeInfo[]
  activeMigrationTasks?: number
  decommissions?: Array<{ nodeId: string; state: string }>
}

/** 集群状态流句柄 */
export interface ClusterStatusStreamHandle {
  stop: () => void
}

/** 获取集群状态快照 */
export async function getRuntimeStatus(): Promise<RuntimeStatus> {
  return get<RuntimeStatus>('/rt/status')
}

/** 获取所有节点列表（含 DEAD 墓碑） */
export async function getClusterNodesApi(): Promise<RuntimeNodeInfo[]> {
  return get<RuntimeNodeInfo[]>('/rt/nodes')
}

/** GET /rt/suspicions 响应：多级故障确认怀疑票视图 */
export interface SuspicionView {
  enabled: boolean
  voteTtlMs: number
  aliveNodes: number
  required: number
  votes: Array<{ nodeId: string; votes: number }>
}

/** 获取怀疑票视图（φ 多级故障确认可观测） */
export async function getSuspicionsApi(): Promise<SuspicionView> {
  return get<SuspicionView>('/rt/suspicions')
}

/** 设置节点能力期望态（ACTIVE/DISABLED） */
export async function setNodeDesiredApi(
  nodeId: string,
  type: string,
  desired: 'ACTIVE' | 'DISABLED'
) {
  return put(`/rt/nodes/${encodeURIComponent(nodeId)}/capabilities/${encodeURIComponent(type)}/desired`, {
    desired
  })
}

/** 分区路由查询 */
export async function placementLookupApi(
  capability: string,
  key: string
): Promise<{
  capability: string
  key: string
  partition: number
  ownerNode: string
  epoch: number
  host: string
  grpcPort: number
  queriedNodeHttpPort: number
}> {
  return get(`/rt/placement/${encodeURIComponent(capability)}/lookup`, { key })
}

// ==================== 调度管理（RuntimeScheduleController）====================

/** 迁移任务（扁平 DTO；sourceCount=-1 表示 SNAPSHOT 前未知） */
export interface MigrationTaskInfo {
  taskId: number
  capability: string
  partition: number
  fromNode: string | null
  toNode: string
  state: string
  checkpointOffset: number
  itemsDone: number
  sourceCount: number
  updatedAtMillis: number
}

/** GET /rt/migration/tasks 响应 */
export interface MigrationTasksResult {
  total: number
  truncated: boolean
  tasks: MigrationTaskInfo[]
}

/** 放置分布：每节点主/副本分区数 */
export interface PlacementDistribution {
  capability: string
  partitionCount: number
  totalPlacements: number
  distribution: Array<{
    nodeId: string
    primaries: number
    replicas: number
  }>
}

/** 每节点 cache 统计（keyCount=null 表示能力未激活；reachable=false 表示不可达） */
export interface CacheNodeStat {
  nodeId: string
  cacheActual: string
  keyCount: number | null
  reachable: boolean
}

/** 每节点存储统计（objectCount=null 表示能力未激活；reachable=false 表示不可达） */
export interface StorageNodeStat {
  nodeId: string
  storageActual: string
  objectCount: number | null
  reachable: boolean
}

/** 迁移任务列表（scope=active 非终态 / all 含终态；taskId 降序，有界） */
export async function getMigrationTasksApi(params: {
  capability?: string
  scope?: 'active' | 'all'
  limit?: number
}): Promise<MigrationTasksResult> {
  return get<MigrationTasksResult>('/rt/migration/tasks', {
    capability: params.capability || undefined,
    scope: params.scope || 'active',
    limit: params.limit || 100
  })
}

/** 取消迁移任务（幂等：终态任务返回 cancelled=false） */
export async function cancelMigrationTaskApi(
  taskId: number
): Promise<{ cancelled: boolean; message: string }> {
  return post(`/rt/migration/tasks/${encodeURIComponent(taskId)}/cancel`)
}

/** 重试失败/中断迁移（FAILED→PENDING 断点续跑） */
export async function resumeMigrationApi(): Promise<{ resumed: boolean }> {
  return post('/rt/migration/resume')
}

/**
 * 手动触发重平衡（重置冷却；leader 亲和——非 leader 节点请求无实际效果，
 * 响应 isLeader/leader 供调用方提示）
 */
export async function triggerRebalanceApi(capability: string): Promise<{
  reset: boolean
  isLeader: boolean
  leader: string | null
}> {
  return post('/rt/scheduler/rebalance', { capability })
}

/** 放置分布（每节点主/副本分区数） */
export async function getPlacementDistributionApi(
  capability: string
): Promise<PlacementDistribution> {
  return get(`/rt/placement/${encodeURIComponent(capability)}/distribution`)
}

/** 每节点 cache 条目统计 */
export async function getCacheNodeStatsApi(): Promise<{ nodes: CacheNodeStat[] }> {
  return get('/rt/cache/node-stats')
}

/** 每节点存储对象统计 */
export async function getStorageNodeStatsApi(): Promise<{ nodes: StorageNodeStat[] }> {
  return get('/rt/storage/node-stats')
}

// ==================== 节点退役（Decommission）====================

/** 退役预检：单能力维度 */
export interface DecommissionCapabilityPrecheck {
  type: string
  desired: string
  actual: string
  primaryPartitions: number
  replicaPartitions: number
  itemCount: number | null
  reachable: boolean
}

/** 退役预检结果 */
export interface DecommissionPrecheck {
  nodeId: string
  nodeState: string
  capabilities: DecommissionCapabilityPrecheck[]
  totalCacheItems: number
  totalStorageObjects: number
  totalPrimaryPartitions: number
  warnings: string[]
  requiresForce: boolean
  degraded: boolean
}

/** 退役进度：单能力维度 */
export interface DecommissionCapabilityProgress {
  type: string
  desired: string
  actual: string
  primaryPartitionsLeft: number
  replicaPartitionsLeft: number
  activeMigrationsFromNode: number
}

/** 退役状态 + 进度 */
export interface DecommissionStatus {
  record: {
    node: string
    state: string
    force: boolean
    detail: string | null
    startedAtMillis: number
    updatedAtMillis: number
    completedAtMillis: number | null
  } | null
  capabilities: DecommissionCapabilityProgress[]
  totalPartitionsLeft: number
  totalPartitionsEstimated: number
}

/** 退役预检（纯读，无副作用） */
export async function decommissionPrecheckApi(nodeId: string): Promise<DecommissionPrecheck> {
  return post<DecommissionPrecheck>(`/rt/nodes/${encodeURIComponent(nodeId)}/decommission/precheck`)
}

/** 启动退役 */
export async function decommissionNodeApi(nodeId: string, force: boolean): Promise<{ nodeId: string; state: string }> {
  return post(`/rt/nodes/${encodeURIComponent(nodeId)}/decommission`, { force })
}

/** 查询退役状态 + 进度 */
export async function getDecommissionStatusApi(nodeId: string): Promise<DecommissionStatus> {
  return get<DecommissionStatus>(`/rt/nodes/${encodeURIComponent(nodeId)}/decommission/status`)
}

const STREAM_RECONNECT_MS = 5000

/** SSE 订阅集群状态流（每秒推送） */
export function startClusterStatusStreamApi(
  onSnapshot: (snapshot: RuntimeStatus) => void,
  onConnectionChange?: (connected: boolean) => void
): ClusterStatusStreamHandle {
  const controller = new AbortController()
  let stopped = false
  let reconnectTimer: ReturnType<typeof setTimeout> | null = null
  let buffer = ''

  const handleFrame = (frame: string) => {
    for (const line of frame.split('\n')) {
      if (!line.startsWith('data:')) continue
      const payload = line.slice(5).trim()
      if (!payload) continue
      try {
        if (payload.startsWith('{')) {
          const data = JSON.parse(payload) as RuntimeStatus
          onSnapshot(data)
          onConnectionChange?.(true)
        }
      } catch {
        // ignore
      }
    }
  }

  const scheduleReconnect = () => {
    if (stopped || reconnectTimer) return
    onConnectionChange?.(false)
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null
      if (!stopped) run()
    }, STREAM_RECONNECT_MS)
  }

  const run = () => {
    buffer = ''
    requestSSE(
      '/rt/status/stream',
      (chunk) => {
        buffer += chunk
        let idx: number
        while ((idx = buffer.indexOf('\n\n')) >= 0) {
          const frame = buffer.slice(0, idx)
          buffer = buffer.slice(idx + 2)
          handleFrame(frame)
        }
      },
      () => scheduleReconnect(),
      () => scheduleReconnect(),
      { signal: controller.signal }
    )
  }

  run()

  return {
    stop: () => {
      stopped = true
      if (reconnectTimer) {
        clearTimeout(reconnectTimer)
        reconnectTimer = null
      }
      controller.abort()
    }
  }
}
