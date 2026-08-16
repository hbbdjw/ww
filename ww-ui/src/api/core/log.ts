import { requestClient } from '#/api/request';

/** 日志条目（历史查询与实时推送共用；历史含 id/createdDate，实时含 timestamp） */
export interface LogItem {
  id?: number;
  level: string;
  loggerName: string;
  threadName: string;
  message: string;
  stackTrace?: null | string;
  node: string;
  createdDate?: string;
  timestamp?: number;
}

/** 分页结果（后端 Page<T> 的 content/total） */
export interface LogPageResult {
  content: LogItem[];
  total: number;
}

/** 可选日志级别 */
export const LOG_LEVELS = ['TRACE', 'DEBUG', 'INFO', 'WARN', 'ERROR'] as const;

/** 历史日志分页筛选查询 */
export async function getLogPageApi(params: {
  pageNumber: number;
  pageSize: number;
  level?: string;
  node?: string;
  keyword?: string;
  startTime?: number;
  endTime?: number;
}): Promise<LogPageResult> {
  return requestClient.get<LogPageResult>('/log/page', { params });
}

/** 按时间段删除日志，返回删除条数 */
export async function deleteLogsByTimeRangeApi(
  startTime: number,
  endTime: number,
): Promise<number> {
  return requestClient.delete<number>('/log/delete', {
    params: { startTime, endTime },
  });
}

/** 实时日志订阅句柄 */
export interface LogStreamHandle {
  stop: () => void;
}

/**
 * 建立实时日志 SSE 订阅（复用 @vben requestSSE：自动带 Authorization、走请求拦截器）。
 * 返回句柄用于停止订阅；SSE 文本帧在此解析为 LogItem 后回调。
 */
export function startLogStreamApi(
  params: { level?: string; node?: string; keyword?: string },
  onLog: (log: LogItem) => void,
  onError?: (error: any) => void,
): LogStreamHandle {
  const controller = new AbortController();
  const query = new URLSearchParams();
  if (params.level) query.set('level', params.level);
  if (params.node) query.set('node', params.node);
  if (params.keyword) query.set('keyword', params.keyword);
  const qs = query.toString();
  const url = qs ? `/log/stream?${qs}` : '/log/stream';

  let buffer = '';
  const handleFrame = (frame: string) => {
    for (const line of frame.split('\n')) {
      if (!line.startsWith('data:')) continue;
      const payload = line.slice(5).trim();
      if (!payload) continue;
      try {
        onLog(JSON.parse(payload) as LogItem);
      } catch {
        // 忽略无法解析的帧
      }
    }
  };

  requestClient
    .requestSSE(url, undefined, {
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
      onEnd: () => {},
    })
    .catch((error) => {
      onError?.(error);
    });

  return { stop: () => controller.abort() };
}
