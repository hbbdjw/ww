import { del, get, requestSSE } from './request'

export interface LogItem {
  id?: number | null
  level: string
  loggerName: string
  threadName?: string
  message: string
  stackTrace?: null | string
  node: string
  timestamp: number
}

export interface LogPageResult {
  total?: number
  page?: number
  size?: number
  hasMore?: boolean
  logs: LogItem[]
}

export const LOG_LEVELS = ['TRACE', 'DEBUG', 'INFO', 'WARN', 'ERROR'] as const

/** 分页查询日志（支持 offset 和 cursor 两种模式） */
export async function getLogPageApi(params: {
  pageNumber?: number
  pageSize: number
  level?: string
  node?: string
  keyword?: string
  beforeTs?: number
  beforeId?: number
}): Promise<LogPageResult> {
  const query: Record<string, any> = { size: params.pageSize }
  if (params.beforeTs != null && params.beforeId != null) {
    query.beforeTs = params.beforeTs
    query.beforeId = params.beforeId
  } else {
    query.page = params.pageNumber ?? 1
  }
  if (params.level) query.level = params.level
  if (params.node) query.node = params.node
  if (params.keyword) query.keyword = params.keyword
  return get<LogPageResult>('/rt/logs', query)
}

export interface LogStreamHandle {
  stop: () => void
}

/** SSE 订阅实时日志流（全集群聚合） */
export function startLogStreamApi(
  params: { level?: string; node?: string; keyword?: string },
  onLog: (log: LogItem) => void,
  onError?: (error: any) => void,
  onConnect?: () => void
): LogStreamHandle {
  const controller = new AbortController()
  const query = new URLSearchParams()
  if (params.level) query.set('level', params.level)
  if (params.node) query.set('node', params.node)
  if (params.keyword) query.set('keyword', params.keyword)
  const qs = query.toString()
  const url = qs ? `/rt/log/stream?${qs}` : '/rt/log/stream'

  let buffer = ''
  const handleFrame = (frame: string) => {
    for (const line of frame.split('\n')) {
      if (!line.startsWith('data:')) continue
      const payload = line.slice(5).trim()
      if (!payload) continue
      try {
        const log = JSON.parse(payload) as LogItem
        if (!log.node) log.node = 'local'
        if (!log.timestamp) log.timestamp = Date.now()
        onLog(log)
      } catch {
        // ignore
      }
    }
  }

  const sseStop = requestSSE(
    url,
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
    (error) => onError?.(error),
    { signal: controller.signal, onConnect }
  )

  return {
    stop: () => {
      sseStop()
      controller.abort()
    }
  }
}

/** 按时间范围清除日志（fromTs 含，toTs 不含），返回删除行数 */
export async function deleteLogsByDateRangeApi(fromTs: number, toTs: number): Promise<{ deleted: number }> {
  return del<{ deleted: number }>('/rt/logs', { fromTs, toTs })
}
