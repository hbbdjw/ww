import { requestSSE } from './request'

// ------------------------------------------------------------------
// 类型
// ------------------------------------------------------------------

/** 单个节点某一秒的请求速率；rps=null 表示该秒无数据（对端不可达/秒桶缺失），折线表现为缺口 */
export interface NodeRatePoint {
  nodeId: string
  state: string
  rps: number | null
}

/** 后端每秒推送的全集群请求速率帧 */
export interface RequestRateSnapshot {
  /** 该秒结束时刻的毫秒时间戳（所有节点共用同一时间轴） */
  timestamp: number
  /** epoch 秒 */
  second: number
  intervalMs: number
  nodes: NodeRatePoint[]
}

export interface MonitorStreamHandle {
  stop: () => void
}

// ------------------------------------------------------------------
// SSE：全集群接口请求速率（每秒一帧，断线自动重连）
// ------------------------------------------------------------------

const STREAM_RECONNECT_MS = 5000

export function startRequestRateStreamApi(
  onSnapshot: (snapshot: RequestRateSnapshot) => void,
  onConnectionChange?: (connected: boolean) => void
): MonitorStreamHandle {
  const controller = new AbortController()
  let stopped = false
  let reconnectTimer: ReturnType<typeof setTimeout> | null = null
  let buffer = ''

  const handleFrame = (frame: string) => {
    for (const line of frame.split('\n')) {
      if (!line.startsWith('data:')) continue
      const payload = line.slice(5).trim()
      if (!payload || !payload.startsWith('{')) continue
      try {
        const data = JSON.parse(payload) as RequestRateSnapshot
        if (data && Array.isArray(data.nodes)) {
          onSnapshot(data)
          onConnectionChange?.(true)
        }
      } catch {
        // 忽略半帧/非 JSON 注释帧
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
      '/rt/metrics/stream',
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
