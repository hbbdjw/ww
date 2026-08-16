import { useAppConfig } from '@vben/hooks';
import { useAccessStore } from '@vben/stores';

import { requestClient } from '#/api/request';

const { apiURL } = useAppConfig(import.meta.env, import.meta.env.PROD);

export interface StorageBucket {
  name: string;
  createdAt: number;
  version: number;
  objectCount: number;
  totalSize: number;
}

export interface BucketStats {
  objectCount: number;
  totalSize: number;
}

export interface FolderEntry {
  type: 'folder';
  name: string;
  prefix: string;
}

export interface FileEntry {
  type: 'file';
  name: string;
  key: string;
  size: number;
  contentType: string;
  etag: string;
  createdAt: number;
  version: number;
}

export type Entry = FileEntry | FolderEntry;

export interface EntryListResult {
  folders: FolderEntry[];
  files: FileEntry[];
  nextMarker: null | string;
  isTruncated: boolean;
  /** 桶内对象数超出后端索引上限（快照被截断）时为 true */
  indexTruncated?: boolean;
  prefix: string;
}

/**
 * 获取存储桶列表（含统计信息）
 * @param refresh 是否强制刷新后端统计快照（数据变更后传 true）
 */
export async function getBucketsApi(
  refresh = false,
): Promise<StorageBucket[]> {
  return requestClient.get<StorageBucket[]>('/storage/admin/buckets', {
    params: { refresh: refresh || undefined },
  });
}

/**
 * 创建存储桶
 */
export async function createBucketApi(name: string) {
  return requestClient.post(`/storage/admin/buckets/${encodeURIComponent(name)}`);
}

/**
 * 删除存储桶
 */
export async function deleteBucketApi(name: string, recursive = false) {
  return requestClient.delete(
    `/storage/admin/buckets/${encodeURIComponent(name)}`,
    { params: { recursive } },
  );
}

/**
 * 获取存储桶统计
 */
export async function getBucketStatsApi(
  bucket: string,
  refresh = false,
): Promise<BucketStats> {
  return requestClient.get<BucketStats>(
    `/storage/admin/buckets/${encodeURIComponent(bucket)}/stats`,
    { params: { refresh: refresh || undefined } },
  );
}

/**
 * 列举指定目录下的文件夹和文件（服务端 marker 游标分页）
 * @param marker  分页游标，传上一页返回的 nextMarker，首页不传
 * @param maxKeys 单页条目数
 * @param refresh 是否强制刷新后端快照（数据变更后的首次请求传 true）
 */
export async function listEntriesApi(
  bucket: string,
  prefix = '',
  marker?: string,
  maxKeys = 200,
  refresh = false,
): Promise<EntryListResult> {
  return requestClient.get<EntryListResult>(
    `/storage/admin/buckets/${encodeURIComponent(bucket)}/entries`,
    {
      params: {
        prefix,
        marker: marker || undefined,
        maxKeys,
        refresh: refresh || undefined,
      },
    },
  );
}

/**
 * 删除文件对象（key 通过 X-Object-Key 请求头传递，值经 encodeURIComponent 编码以支持中文/特殊字符）
 */
export async function deleteObjectApi(bucket: string, key: string) {
  return requestClient.delete(
    `/storage/buckets/${encodeURIComponent(bucket)}/object`,
    { headers: { 'X-Object-Key': encodeURIComponent(key) } },
  );
}

/**
 * 删除文件夹（递归删除该前缀下所有对象）
 * @param bucket 存储桶名
 * @param prefix 文件夹前缀（必须以 / 结尾，如 "docs/reports/"）
 */
export async function deleteFolderApi(bucket: string, prefix: string) {
  return requestClient.delete(
    `/storage/admin/buckets/${encodeURIComponent(bucket)}/folder`,
    { params: { prefix } },
  );
}

/**
 * 新建目录（创建以 / 结尾的空对象占位，key 通过 X-Object-Key 请求头传递，值经 encodeURIComponent 编码）
 * @param bucket 存储桶名
 * @param prefix 当前目录前缀
 * @param folderName 新目录名（不含 /）
 */
export async function createFolderApi(
  bucket: string,
  prefix: string,
  folderName: string,
) {
  const normalizedPrefix = prefix.endsWith('/') || prefix === ''
    ? prefix
    : `${prefix}/`;
  const folderKey = `${normalizedPrefix}${folderName}/`;
  return requestClient.put(
    `/storage/buckets/${encodeURIComponent(bucket)}/object`,
    '',
    {
      headers: {
        'Content-Type': 'application/x-directory',
        'X-Object-Key': encodeURIComponent(folderKey),
      },
      timeout: 30000,
    },
  );
}

/**
 * 上传文件（key 通过 X-Object-Key 请求头传递，值经 encodeURIComponent 编码以支持中文/全角等特殊字符）
 * @param bucket 存储桶名
 * @param prefix 当前目录前缀
 * @param file 文件对象
 * @param onProgress 上传进度回调（0-100）
 */
export async function uploadFileApi(
  bucket: string,
  prefix: string,
  file: File,
  onProgress?: (progress: number) => void,
) {
  const normalizedPrefix = prefix.endsWith('/') || prefix === ''
    ? prefix
    : `${prefix}/`;
  const objectKey = `${normalizedPrefix}${file.name}`;
  return requestClient.put(
    `/storage/buckets/${encodeURIComponent(bucket)}/object`,
    file,
    {
      headers: {
        'Content-Type': file.type || 'application/octet-stream',
        'X-Object-Key': encodeURIComponent(objectKey),
      },
      timeout: 0,
      onUploadProgress: (e: any) => {
        if (onProgress && e.total) {
          onProgress(Math.round((e.loaded / e.total) * 100));
        }
      },
    },
  );
}

/**
 * 下载文件（返回 Blob，key 通过 X-Object-Key 请求头传递，值经 encodeURIComponent 编码以支持中文/特殊字符）
 * @param onProgress 可选的下载进度回调，参数为已下载字节数和总字节数
 */
export async function downloadObjectApi(
  bucket: string,
  key: string,
  onProgress?: (loaded: number, total: number) => void,
): Promise<Blob> {
  return requestClient.get(
    `/storage/buckets/${encodeURIComponent(bucket)}/object`,
    {
      headers: { 'X-Object-Key': encodeURIComponent(key) },
      // 缓存破解：所有文件共享同一 URL（仅 X-Object-Key 请求头不同），
      // 浏览器缓存键不包含自定义请求头，会导致查看/下载时返回上次的错误文件
      params: { _t: Date.now() },
      responseType: 'blob',
      responseReturn: 'body',
      timeout: 0,
      onDownloadProgress: (e) => {
        const total = e.total ?? 0;
        if (onProgress && total > 0) {
          onProgress(e.loaded, total);
        }
      },
    },
  );
}

/**
 * 获取文本文件内容（用于预览）
 */
export async function getTextContentApi(
  bucket: string,
  key: string,
): Promise<string> {
  const blob = await downloadObjectApi(bucket, key);
  return blob.text();
}

/**
 * 获取文件的下载 URL（用于图片预览，通过 Blob URL）
 */
export async function getObjectBlobUrlApi(
  bucket: string,
  key: string,
): Promise<string> {
  const blob = await downloadObjectApi(bucket, key);
  return URL.createObjectURL(blob);
}

/**
 * 构造对象的流式直链（供原生 <video>/<audio> 边下边播，依赖后端 Range 支持）
 * <video> 标签无法附加 Authorization / X-Object-Key 请求头，因此：
 *  - key 通过 S3 兼容路径端点 /buckets/{bucket}/objects/** 传递（逐段 URL 编码，保留路径分隔符）
 *  - 令牌通过 access_token URL 参数传递（后端 DualAuthFilter 仅在
 *    /storage/buckets/** 的 GET/HEAD 端点接受该方式）
 */
export function getObjectStreamUrl(bucket: string, key: string): string {
  const accessStore = useAccessStore();
  const encodedKey = key
    .split('/')
    .map((segment) => encodeURIComponent(segment))
    .join('/');
  const token = encodeURIComponent(accessStore.accessToken || '');
  return `${apiURL}/storage/buckets/${encodeURIComponent(bucket)}/objects/${encodedKey}?access_token=${token}`;
}

/**
 * 格式化文件大小为人类可读字符串
 */
export function formatFileSize(bytes: number): string {
  if (bytes <= 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  const size = bytes / Math.pow(1024, i);
  return `${size.toFixed(i === 0 ? 0 : 2)} ${units[i]}`;
}

/**
 * 格式化时间戳为日期字符串
 */
export function formatDate(timestamp: number): string {
  if (!timestamp) return '-';
  const d = new Date(timestamp);
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

/**
 * 根据文件名/ContentType 判断文件类型类别
 */
export type FileCategory =
  | 'archive'
  | 'audio'
  | 'code'
  | 'file'
  | 'image'
  | 'pdf'
  | 'text'
  | 'video';

export function getFileCategory(
  name: string,
  contentType?: string,
): FileCategory {
  const ext = name.toLowerCase().split('.').pop() || '';
  if (contentType) {
    if (contentType.startsWith('image/')) return 'image';
    if (contentType.startsWith('video/')) return 'video';
    if (contentType.startsWith('audio/')) return 'audio';
    if (contentType === 'application/pdf') return 'pdf';
    if (contentType.startsWith('text/')) return 'text';
  }
  if (['bmp', 'gif', 'ico', 'jpeg', 'jpg', 'png', 'svg', 'webp'].includes(ext))
    return 'image';
  if (['avi', 'flv', 'mkv', 'mov', 'mp4', 'webm', 'wmv'].includes(ext))
    return 'video';
  if (['aac', 'flac', 'm4a', 'mp3', 'ogg', 'wav'].includes(ext))
    return 'audio';
  if (ext === 'pdf') return 'pdf';
  if (['7z', 'bz2', 'gz', 'rar', 'tar', 'zip'].includes(ext))
    return 'archive';
  if (['conf', 'csv', 'ini', 'json', 'log', 'md', 'txt', 'xml', 'yaml', 'yml'].includes(ext))
    return 'text';
  if (['bat', 'c', 'cpp', 'css', 'go', 'h', 'html', 'java', 'js', 'jsx', 'py', 'rs', 'sh', 'sql', 'ts', 'tsx', 'vue'].includes(ext))
    return 'code';
  return 'file';
}

/**
 * 判断文件是否可预览（图片或文本）
 */
export function isPreviewable(name: string, contentType?: string): boolean {
  const cat = getFileCategory(name, contentType);
  return cat === 'image' || cat === 'text' || cat === 'pdf' || cat === 'code';
}
