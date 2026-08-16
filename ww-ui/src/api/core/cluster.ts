import { requestClient } from '#/api/request';

// ==================== 类型定义 ====================

/** 集群节点基础信息 */
export interface ClusterNodeInfo {
  nodeKey: string;
  host: string;
  port: number;
  weight: number;
  self: boolean;
  status: 'ALIVE' | 'DEAD' | 'SUSPECT';
  epoch: number;
  lastHeartbeatMs: number;
  storageEnabled: boolean;
  cacheEnabled: boolean;
}

/** 集群状态总览（/cluster/status） */
export interface ClusterStatusOverview {
  membershipEnabled?: boolean;
  topologyVersion: number;
  aliveCount: number;
  selfNodeKey: string;
  nodes: ClusterNodeInfo[];
  failover: Record<string, any>;
  rebalance: Record<string, any>;
}

/** 节点视图（/cluster/nodes 返回，含在线探测与指标） */
export interface ClusterNodeView {
  nodeKey: string;
  host: string;
  port: number;
  weight: number;
  self: boolean;
  status: string;
  epoch: number;
  lastHeartbeatMs: number;
  storageEnabled: boolean;
  cacheEnabled: boolean;
  online: boolean;
  latencyMs: number;
  metrics: Record<string, any>;
  error?: string;
}

/** 节点总览（/cluster/nodes 响应） */
export interface ClusterNodesOverview {
  timestamp: number;
  topologyVersion: number;
  selfNodeKey: string;
  nodes: ClusterNodeView[];
}

/** 存储集群拓扑节点 */
export interface StorageTopologyNode {
  nodeKey: string;
  /** cache 侧节点键（host:cachePort），用于管理面将存储节点与缓存节点合并为同一物理节点 */
  cacheNodeKey?: string;
  host: string;
  httpPort: number;
  weight: number;
  self: boolean;
  storageEnabled: boolean;
  cacheEnabled: boolean;
}

/** 存储集群拓扑（/storage/cluster/topology） */
export interface StorageClusterTopology {
  version: number;
  replicas: number;
  nodes: StorageTopologyNode[];
}

/** 存储节点健康快照 */
export interface StorageHealthNode {
  nodeKey: string;
  /** cache 侧节点键（host:cachePort），用于管理面将存储节点与缓存节点合并为同一物理节点 */
  cacheNodeKey?: string;
  host: string;
  httpPort: number;
  self: boolean;
  status: 'ALIVE' | 'DEAD' | 'QUARANTINE' | 'SUSPECT';
  failures: number;
}

/** 存储集群健康状态 */
export interface StorageClusterHealth {
  epoch: number;
  nodes: StorageHealthNode[];
}

/** SSE 聚合快照（/cluster/status/stream 每帧 data 载荷） */
export interface ClusterStreamSnapshot {
  timestamp: number;
  overview: ClusterStatusOverview & { error?: string };
  cacheNodes: ClusterNodesOverview & { error?: string };
  storage: {
    enabled: boolean;
    maintenance?: boolean;
    topology?: { error?: string } & StorageClusterTopology;
    health?: { error?: string } & StorageClusterHealth;
  };
}

/** 集群状态 SSE 订阅句柄 */
export interface ClusterStatusStreamHandle {
  stop: () => void;
}

// ==================== 缓存集群 API（/cluster/*） ====================

/** 获取集群状态总览 */
export async function getClusterStatusApi(): Promise<ClusterStatusOverview> {
  return requestClient.post<ClusterStatusOverview>('/cluster/status');
}

/** 获取集群节点总览（含在线探测与指标） */
export async function getClusterNodesApi(): Promise<ClusterNodesOverview> {
  return requestClient.post<ClusterNodesOverview>('/cluster/nodes');
}

/** 手动 failover 指定节点 */
export async function clusterFailoverApi(nodeKey: string) {
  return requestClient.post('/cluster/failover', { nodeKey });
}

/** 取消手动 failover */
export async function clusterCancelFailoverApi(nodeKey: string) {
  return requestClient.post('/cluster/cancelFailover', { nodeKey });
}

/** 转移 master 角色 */
export async function clusterTransferMasterApi(
  sourceNodeKey: string,
  targetNodeKey?: string,
) {
  return requestClient.post('/cluster/transferMaster', {
    sourceNodeKey,
    targetNodeKey,
  });
}

/** 节点加入集群 */
export async function clusterJoinApi(
  host: string,
  port: number,
  weight = 1.0,
) {
  return requestClient.post('/cluster/join', { host, port, weight });
}

/** 节点退出集群 */
export async function clusterLeaveApi(host: string, port: number) {
  return requestClient.post('/cluster/leave', { host, port });
}

/** 标记节点 ALIVE */
export async function clusterAliveApi(nodeKey: string) {
  return requestClient.post('/cluster/alive', { nodeKey });
}

/** 标记节点 SUSPECT */
export async function clusterSuspectApi(nodeKey: string) {
  return requestClient.post('/cluster/suspect', { nodeKey });
}

/** 触发数据重平衡 */
export async function clusterRebalanceApi() {
  return requestClient.post('/cluster/rebalance');
}

/** 切换节点存储/缓存能力 */
export async function toggleNodeCapabilityApi(
  nodeKey: string,
  capability: 'cache' | 'storage',
  enabled: boolean,
) {
  return requestClient.post('/cluster/capability', {
    nodeKey,
    capability,
    enabled,
  });
}

// ==================== 存储集群 API（/storage/cluster/*） ====================

/** 获取存储集群拓扑 */
export async function getStorageClusterTopologyApi(): Promise<StorageClusterTopology> {
  return requestClient.get<StorageClusterTopology>('/storage/cluster/topology');
}

/** 获取存储集群健康状态 */
export async function getStorageClusterHealthApi(): Promise<StorageClusterHealth> {
  return requestClient.get<StorageClusterHealth>('/storage/cluster/health');
}

/** 获取存储维护态状态 */
export async function getStorageMaintenanceApi(): Promise<{ maintenance: boolean }> {
  return requestClient.get<{ maintenance: boolean }>(
    '/storage/cluster/maintenance',
  );
}

/** 设置存储维护态 */
export async function storageMaintenanceApi(enabled: boolean) {
  return requestClient.put('/storage/cluster/maintenance', null, {
    params: { enabled },
  });
}

/** 存储节点扩容 */
export async function storageScaleOutApi(
  host: string,
  httpPort: number,
  weight = 1.0,
  rebalance = true,
) {
  return requestClient.post('/storage/cluster/scale-out', null, {
    params: { host, httpPort, weight, rebalance },
  });
}

/** 存储节点数据迁移（转移） */
export async function storageTransferApi(sourceNodeKey: string) {
  return requestClient.post('/storage/cluster/transfer', null, {
    params: { sourceNodeKey },
  });
}

/** 存储节点合并 */
export async function storageMergeApi(
  sourceNodeKey: string,
  purgeSource = true,
) {
  return requestClient.post('/storage/cluster/merge', null, {
    params: { sourceNodeKey, purgeSource },
  });
}

// ==================== 集群状态实时推送（SSE） ====================

/** SSE 断连后自动重连间隔（毫秒） */
const STREAM_RECONNECT_MS = 5000;

/**
 * 订阅集群状态实时流（复用 requestSSE：自动带 Authorization、走请求拦截器）。
 * <p>后端每帧推送聚合快照（overview + cacheNodes + storage）；
 * 连接断开（onEnd/onError 且非主动 stop）时自动重连。
 * 返回句柄用于停止订阅。
 */
export function startClusterStatusStreamApi(
  onSnapshot: (snapshot: ClusterStreamSnapshot) => void,
  onConnectionChange?: (connected: boolean) => void,
): ClusterStatusStreamHandle {
  const controller = new AbortController();
  let stopped = false;
  let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  let buffer = '';

  const handleFrame = (frame: string) => {
    for (const line of frame.split('\n')) {
      if (!line.startsWith('data:')) continue;
      const payload = line.slice(5).trim();
      if (!payload) continue;
      try {
        onSnapshot(JSON.parse(payload) as ClusterStreamSnapshot);
        onConnectionChange?.(true);
      } catch {
        // 忽略无法解析的帧
      }
    }
  };

  const scheduleReconnect = () => {
    if (stopped || reconnectTimer) return;
    onConnectionChange?.(false);
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      if (!stopped) run();
    }, STREAM_RECONNECT_MS);
  };

  const run = () => {
    buffer = '';
    requestClient
      .requestSSE('/cluster/status/stream', undefined, {
        signal: controller.signal,
        onMessage: (chunk) => {
          buffer += chunk;
          let idx: number;
          while ((idx = buffer.indexOf('\n\n')) >= 0) {
            const frame = buffer.slice(0, idx);
            buffer = buffer.slice(idx + 2);
            handleFrame(frame);
          }
        },
        onEnd: () => {
          scheduleReconnect();
        },
      })
      .catch(() => {
        scheduleReconnect();
      });
  };

  run();

  return {
    stop: () => {
      stopped = true;
      if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
      }
      controller.abort();
    },
  };
}
