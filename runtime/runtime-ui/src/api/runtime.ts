import { get, requestSSE } from './request'

export interface MenuNode {
  type: 'item' | 'header' | 'tree'
  text: string
  href?: string
  icon?: string
  key?: string
  badge?: string | number
  badgeColor?: string
  children?: MenuNode[]
}

export interface NodeInfo {
  nodeId: string
  host: string
  grpcPort: number
  state: string
  stateEpoch?: number
  weight?: number
}

export interface CapabilityRecord {
  nodeId: string
  type: string
  desired: string
  actual: string
  updatedAtMillis?: number
}

export interface ClusterStatus {
  nodeId: string
  leader: string | null
  isLeader?: boolean
  leaseOwner: string | null
  capabilities: CapabilityRecord[]
  primariesByCapability: Record<string, Record<string, string>>
  self: NodeInfo | null
  members: NodeInfo[]
  activeMigrationTasks: number
}

export function getStatus(): Promise<ClusterStatus> {
  return get<ClusterStatus>('/rt/status')
}

export function subscribeStatus(
  onStatus: (status: ClusterStatus) => void,
  onError?: (err: Event) => void
): () => void {
  let buffer = ''
  const handleFrame = (frame: string) => {
    for (const line of frame.split('\n')) {
      if (!line.startsWith('data:')) continue
      const payload = line.slice(5).trim()
      if (!payload) continue
      if (payload.startsWith('{')) {
        try {
          onStatus(JSON.parse(payload) as ClusterStatus)
        } catch (e) {
          // ignore
        }
      }
    }
  }

  return requestSSE(
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
    () => {},
    (e) => onError?.(e as Event)
  )
}

export function getNodes(): Promise<NodeInfo[]> {
  return get<NodeInfo[]>('/rt/nodes')
}

export function setDesiredState(
  nodeId: string,
  type: string,
  desired: string
): Promise<CapabilityRecord> {
  return get<CapabilityRecord>(
    `/rt/nodes/${nodeId}/capabilities/${type}/desired`,
    { desired }
  )
}

export function lookupPlacement(capability: string, key: string): Promise<any> {
  return get(`/rt/placement/${capability}/lookup`, { key })
}
