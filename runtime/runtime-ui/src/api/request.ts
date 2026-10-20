const BASE_URL = ''

import { useAuth } from '@/composables/useAuth'

export interface RequestOptions extends RequestInit {
  params?: Record<string, any>
  timeout?: number
}

async function parseResponse<T>(response: Response): Promise<T> {
  const contentType = response.headers.get('content-type') || ''
  if (contentType.includes('application/json')) {
    return response.json()
  }
  return response.text() as unknown as T
}

export async function request<T = any>(url: string, options: RequestOptions = {}): Promise<T> {
  const { params, timeout = 30000, ...fetchOptions } = options

  let fullUrl = BASE_URL + url
  if (params) {
    const searchParams = new URLSearchParams()
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== '') {
        searchParams.append(key, String(value))
      }
    })
    const qs = searchParams.toString()
    if (qs) fullUrl += (fullUrl.includes('?') ? '&' : '?') + qs
  }

  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), timeout)

  try {
    const response = await fetch(fullUrl, {
      ...fetchOptions,
      headers: {
        'Content-Type': 'application/json',
        ...authHeaders(),
        ...(fetchOptions.headers || {})
      },
      // 管理控制台要求读后写一致：禁用浏览器 HTTP 缓存，避免列表/统计读到旧数据
      cache: 'no-store',
      signal: controller.signal
    })

    clearTimeout(timer)

    if (response.status === 401) {
      handleUnauthorized()
    }

    if (!response.ok) {
      let errorMessage = `API error: ${response.status}`
      try {
        const errorBody = await response.json()
        errorMessage = errorBody.message || errorBody.error || errorMessage
      } catch {}
      const error = new Error(errorMessage) as Error & { status?: number }
      error.status = response.status
      throw error
    }

    return parseResponse<T>(response)
  } catch (e: any) {
    clearTimeout(timer)
    if (e.name === 'AbortError') {
      throw new Error('请求超时')
    }
    throw e
  }
}

/** 认证头：已登录时附加 Bearer token。原生 XHR/媒体流等绕过 fetch 封装的请求需手动复用。 */
export function authHeaders(): Record<string, string> {
  const value = useAuth().token.value
  return value ? { Authorization: `Bearer ${value}` } : {}
}

/** 401 处理：清除凭证并跳登录页（已在登录页时不跳转，错误体照常抛出供表单提示）。 */
export function handleUnauthorized() {
  const auth = useAuth()
  auth.clearAuth()
  const onLoginPage = window.location.hash.startsWith('#/login')
  if (!onLoginPage) {
    window.location.hash = '#/login'
  }
}

/**
 * 为无法附加自定义请求头的浏览器原生元素（img/video/audio/iframe 的 src）
 * 追加 access_token 查询参数，令牌由后端在 GET/HEAD 上回退识别。
 * 仅限幂等读取 URL 使用，切勿用于写操作。
 */
export function withAccessToken(url: string): string {
  const value = useAuth().token.value
  if (!value) return url
  return `${url}${url.includes('?') ? '&' : '?'}access_token=${encodeURIComponent(value)}`
}

export function get<T = any>(url: string, params?: Record<string, any>, options?: RequestOptions): Promise<T> {
  return request<T>(url, { ...options, method: 'GET', params })
}

export function post<T = any>(url: string, data?: any, options?: RequestOptions): Promise<T> {
  return request<T>(url, {
    ...options,
    method: 'POST',
    body: data !== undefined ? (typeof data === 'string' ? data : JSON.stringify(data)) : undefined
  })
}

export function put<T = any>(url: string, data?: any, options?: RequestOptions): Promise<T> {
  return request<T>(url, {
    ...options,
    method: 'PUT',
    body: data !== undefined ? (typeof data === 'string' ? data : JSON.stringify(data)) : undefined
  })
}

export function del<T = any>(url: string, params?: Record<string, any>, options?: RequestOptions): Promise<T> {
  return request<T>(url, { ...options, method: 'DELETE', params })
}

export interface SSEHandle {
  stop: () => void
}

export function requestSSE(
  url: string,
  onMessage: (chunk: string) => void,
  onEnd?: () => void,
  onError?: (error: any) => void,
  options?: { signal?: AbortSignal; onConnect?: () => void }
): () => void {
  const controller = new AbortController()
  const signal = options?.signal || controller.signal
  let stopped = false

  const run = async () => {
    try {
      const response = await fetch(BASE_URL + url, {
        method: 'GET',
        headers: {
          'Accept': 'text/event-stream',
          'Cache-Control': 'no-cache',
          ...authHeaders()
        },
        signal
      })

      if (!response.ok || !response.body) {
        onError?.(new Error(`SSE error: ${response.status}`))
        onEnd?.()
        return
      }

      options?.onConnect?.()

      const reader = response.body.getReader()
      const decoder = new TextDecoder()

      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        if (stopped) break
        const chunk = decoder.decode(value, { stream: true })
        onMessage(chunk)
      }

      if (!stopped) {
        onEnd?.()
      }
    } catch (e: any) {
      if (!stopped && e.name !== 'AbortError') {
        onError?.(e)
      }
      if (!stopped) {
        onEnd?.()
      }
    }
  }

  run()

  return () => {
    stopped = true
    controller.abort()
  }
}

// ==================== Message 工具（简单实现，待完善）====================
export const message = {
  success: (msg: string) => console.log('[SUCCESS]', msg),
  error: (msg: string) => console.error('[ERROR]', msg),
  warning: (msg: string) => console.warn('[WARNING]', msg),
  info: (msg: string) => console.info('[INFO]', msg)
}
