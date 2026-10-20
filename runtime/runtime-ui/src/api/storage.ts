import { authHeaders, get, handleUnauthorized, put, del, withAccessToken } from './request'

/** 对象元数据 */
export interface ObjectMetadata {
  key: string
  size: number
  contentType: string
  etag: string
  createdAt: number
  version: number
  userMetadata?: Record<string, string>
}

/** GET /storage 响应（桶摘要；stats=true 时带统计字段） */
export interface BucketSummary {
  name: string
  createdAt: number
  objectCount?: number
  totalSize?: number
  /** true 表示对象数超过 10 万，计数为近似值 */
  objectCountEstimate?: boolean
}

/** 列桶（stats=true 时聚合对象数与总大小） */
export async function listBucketsApi(stats = false): Promise<BucketSummary[]> {
  const result = await get<{ buckets: BucketSummary[] }>('/storage', stats ? { stats: 'true' } : undefined)
  return result.buckets ?? []
}

/** 建桶（幂等；桶名 3-63 位小写字母/数字/中划线） */
export async function createBucketApi(name: string): Promise<void> {
  await put(`/storage/${encodeURIComponent(name)}`)
}

/** 删桶（recursive=true 强制清空；非空桶不递归时后端返回 409） */
export async function deleteBucketApi(name: string, recursive = false): Promise<void> {
  await del(`/storage/${encodeURIComponent(name)}`, recursive ? { recursive: 'true' } : undefined)
}

/** GET /storage/{bucket} 响应 */
export interface ListObjectsResult {
  bucket: string
  objects: ObjectMetadata[]
  nextMarker: string | null
  truncated: boolean
}

/** 文件夹条目（前端虚拟） */
export interface FolderEntry {
  type: 'folder'
  name: string
  prefix: string
}

/** 文件条目 */
export interface FileEntry {
  type: 'file'
  name: string
  key: string
  size: number
  contentType: string
  etag: string
  createdAt: number
  version: number
}

export type Entry = FileEntry | FolderEntry

/** 列举桶内对象（S3 风格） */
export async function listObjectsApi(
  bucket: string,
  prefix = '',
  marker?: string,
  maxKeys = 200
): Promise<ListObjectsResult> {
  return get<ListObjectsResult>(
    `/storage/${encodeURIComponent(bucket)}`,
    {
      prefix: prefix || undefined,
      marker: marker || undefined,
      maxKeys
    }
  )
}

const UPLOAD_TIMEOUT = 3_600_000 // 1 小时超时（支持大文件）

/** 上传对象（流式 PUT，已配置 1 小时超时和详细错误信息） */
export async function uploadObjectApi(
  bucket: string,
  key: string,
  file: File | Blob,
  contentType?: string,
  onProgress?: (progress: number) => void
): Promise<void> {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest()
    const url = `/storage/${encodeURIComponent(bucket)}/${encodeKey(key)}`
    xhr.open('PUT', url)
    xhr.timeout = UPLOAD_TIMEOUT
    xhr.setRequestHeader('Content-Type', contentType || (file instanceof File ? file.type : '') || 'application/octet-stream')
    // XHR 绕过了 fetch 封装，需手动附加 Bearer 令牌，否则后端按未认证返回 401
    const auth = authHeaders()
    if (auth.Authorization) {
      xhr.setRequestHeader('Authorization', auth.Authorization)
    }

    xhr.upload.onprogress = (e) => {
      if (onProgress && e.lengthComputable) {
        onProgress(Math.round((e.loaded / e.total) * 100))
      }
    }

    xhr.onload = () => {
      if (xhr.status >= 200 && xhr.status < 300) {
        resolve()
      } else {
        if (xhr.status === 401) {
          handleUnauthorized()
        }
        let msg = `Upload failed: ${xhr.status} ${xhr.statusText}`
        try {
          const resp = xhr.responseText
          if (resp) {
            try {
              const err = JSON.parse(resp)
              msg = err.message || err.error || msg
            } catch {
              // 不是 JSON，直接用文本
              msg = resp.slice(0, 500) || msg
            }
          }
        } catch {}
        reject(new Error(msg))
      }
    }

    xhr.ontimeout = () => reject(new Error('上传超时（超过 1 小时），请检查网络连接或尝试分片上传小文件'))
    xhr.onabort = () => reject(new Error('Upload aborted'))
    xhr.onerror = () => {
      let msg = 'Upload network error (网络连接中断或跨域问题)'
      try {
        if (xhr.status) {
          msg = `上传失败: HTTP ${xhr.status} ${xhr.statusText || ''}`
        }
      } catch {}
      reject(new Error(msg))
    }
    xhr.send(file)
  })
}

/** 下载对象（返回 Blob） */
export async function downloadObjectApi(
  bucket: string,
  key: string
): Promise<Blob> {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest()
    const url = `/storage/${encodeURIComponent(bucket)}/${encodeKey(key)}?_t=${Date.now()}`
    xhr.open('GET', url)
    xhr.responseType = 'blob'
    // 与上传同理：手动附加 Bearer 令牌
    const auth = authHeaders()
    if (auth.Authorization) {
      xhr.setRequestHeader('Authorization', auth.Authorization)
    }

    xhr.onload = () => {
      if (xhr.status >= 200 && xhr.status < 300) {
        resolve(xhr.response)
      } else {
        if (xhr.status === 401) {
          handleUnauthorized()
        }
        reject(new Error(`Download failed: ${xhr.status}`))
      }
    }

    xhr.onerror = () => reject(new Error('Download network error'))
    xhr.send()
  })
}

/** 获取对象文本内容 */
export async function getTextContentApi(bucket: string, key: string): Promise<string> {
  const blob = await downloadObjectApi(bucket, key)
  return blob.text()
}

/**
 * 媒体直连流式 URL：浏览器自行发起 Range 请求，边下边播、可拖动定位
 * （后端支持 206 Partial Content + Accept-Ranges: bytes）。
 * ?_t 防止覆盖上传后命中旧缓存；后端 extractKey 基于请求路径，对 query 透明。
 * 媒体元素无法附加 Authorization 头，以 access_token 查询参数携带会话令牌
 * （后端仅在 GET/HEAD 上回退识别该参数）。
 */
export function getObjectStreamUrl(bucket: string, key: string): string {
  return withAccessToken(
    `/storage/${encodeURIComponent(bucket)}/${encodeKey(key)}?_t=${Date.now()}`
  )
}

/** 创建"文件夹"（放一个空对象 key 以 / 结尾） */
export async function createFolderApi(
  bucket: string,
  prefix: string,
  folderName: string
) {
  const normalizedPrefix = prefix.endsWith('/') || prefix === '' ? prefix : `${prefix}/`
  const folderKey = `${normalizedPrefix}${folderName}/`
  return put(
    `/storage/${encodeURIComponent(bucket)}/${encodeKey(folderKey)}`,
    '',
    {
      headers: { 'Content-Type': 'application/x-directory' },
      timeout: 30000
    }
  )
}

/** 删除对象 */
export async function deleteObjectApi(bucket: string, key: string) {
  return del(`/storage/${encodeURIComponent(bucket)}/${encodeKey(key)}`)
}

/** 递归删除"文件夹"下所有对象 */
export async function deleteFolderApi(bucket: string, prefix: string) {
  let marker: string | undefined
  const keys: string[] = []
  do {
    const result = await listObjectsApi(bucket, prefix, marker, 1000)
    for (const obj of result.objects) {
      keys.push(obj.key)
    }
    marker = result.nextMarker || undefined
  } while (marker)

  for (const key of keys) {
    await deleteObjectApi(bucket, key)
  }
}

/**
 * 将扁平对象列表解析为文件夹 + 文件结构。
 * 与 S3 控制台类似：key 中 prefix 之后的第一段 / 之前的部分作为文件夹。
 */
export function parseEntries(
  objects: ObjectMetadata[],
  currentPrefix: string
): { folders: FolderEntry[]; files: FileEntry[] } {
  const folderMap = new Map<string, FolderEntry>()
  const files: FileEntry[] = []

  for (const obj of objects) {
    // 如果 key 等于当前 prefix，跳过（这是"文件夹"自身的占位对象）
    if (obj.key === currentPrefix) continue

    // 去掉当前 prefix，得到相对路径
    const relative = obj.key.startsWith(currentPrefix)
      ? obj.key.slice(currentPrefix.length)
      : obj.key

    const slashIdx = relative.indexOf('/')
    if (slashIdx >= 0) {
      // 有子路径 → 文件夹
      const folderName = relative.slice(0, slashIdx)
      const folderPrefix = currentPrefix + folderName + '/'
      if (!folderMap.has(folderName)) {
        folderMap.set(folderName, {
          type: 'folder',
          name: folderName,
          prefix: folderPrefix
        })
      }
    } else {
      // 无子路径 → 文件
      files.push({
        type: 'file',
        name: relative,
        key: obj.key,
        size: obj.size,
        contentType: obj.contentType,
        etag: obj.etag,
        createdAt: obj.createdAt,
        version: obj.version
      })
    }
  }

  return {
    folders: Array.from(folderMap.values()).sort((a, b) => a.name.localeCompare(b.name)),
    files: files.sort((a, b) => a.name.localeCompare(b.name))
  }
}

/** 对 key 中的 / 不做 encode（路径分隔符），仅编码其他特殊字符 */
function encodeKey(key: string): string {
  return key.split('/').map(encodeURIComponent).join('/')
}

export function formatFileSize(bytes: number): string {
  if (bytes <= 0) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  const i = Math.floor(Math.log(bytes) / Math.log(1024))
  const size = bytes / Math.pow(1024, i)
  return `${size.toFixed(i === 0 ? 0 : 2)} ${units[i]}`
}

export function formatDate(timestamp: number): string {
  if (!timestamp) return '-'
  const d = new Date(timestamp)
  const pad = (n: number) => String(n).padStart(2, '0')
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`
}

export type FileCategory =
  | 'archive'
  | 'audio'
  | 'code'
  | 'file'
  | 'image'
  | 'pdf'
  | 'text'
  | 'video'

export function getFileCategory(name: string, contentType?: string): FileCategory {
  const ext = name.toLowerCase().split('.').pop() || ''
  if (contentType) {
    if (contentType.startsWith('image/')) return 'image'
    if (contentType.startsWith('video/')) return 'video'
    if (contentType.startsWith('audio/')) return 'audio'
    if (contentType === 'application/pdf') return 'pdf'
    if (contentType.startsWith('text/')) return 'text'
  }
  if (['bmp', 'gif', 'ico', 'jpeg', 'jpg', 'png', 'svg', 'webp'].includes(ext)) return 'image'
  if (['avi', 'flv', 'mkv', 'mov', 'mp4', 'webm', 'wmv'].includes(ext)) return 'video'
  if (['aac', 'flac', 'm4a', 'mp3', 'ogg', 'wav'].includes(ext)) return 'audio'
  if (ext === 'pdf') return 'pdf'
  if (['7z', 'bz2', 'gz', 'rar', 'tar', 'zip'].includes(ext)) return 'archive'
  if (['conf', 'csv', 'ini', 'json', 'log', 'md', 'txt', 'xml', 'yaml', 'yml'].includes(ext)) return 'text'
  if (['bat', 'c', 'cpp', 'css', 'go', 'h', 'html', 'java', 'js', 'jsx', 'py', 'rs', 'sh', 'sql', 'ts', 'tsx', 'vue'].includes(ext)) return 'code'
  return 'file'
}

export function isPreviewable(name: string, contentType?: string): boolean {
  const cat = getFileCategory(name, contentType)
  return (
    cat === 'image' ||
    cat === 'text' ||
    cat === 'pdf' ||
    cat === 'code' ||
    cat === 'video' ||
    cat === 'audio'
  )
}
