<script setup lang="ts">
import { computed, h, onMounted, ref } from 'vue'
import {
  NButton,
  NButtonGroup,
  NDataTable,
  NEmpty,
  NInput,
  NPopconfirm,
  NProgress,
  NSelect,
  NSpin,
  NTooltip,
  useDialog,
  useMessage,
  type DataTableColumns
} from 'naive-ui'
import WwModal from '@/components/WwModal.vue'
import {
  createBucketApi,
  createFolderApi,
  deleteBucketApi,
  deleteFolderApi,
  deleteObjectApi,
  downloadObjectApi,
  type BucketSummary,
  type FileEntry,
  type FolderEntry,
  formatDate,
  formatFileSize,
  getFileCategory,
  getObjectStreamUrl,
  getTextContentApi,
  isPreviewable,
  listBucketsApi,
  listObjectsApi,
  parseEntries,
  uploadObjectApi
} from '@/api/storage'
import FileIcon from '@/components/FileIcon.vue'
import ActionIcon from '@/components/ActionIcon.vue'

defineOptions({ name: 'StorageManage' })

const message = useMessage()
const dialog = useDialog()

// ==================== 存储桶 ====================
const buckets = ref<BucketSummary[]>([])
const bucketsLoading = ref(false)
const selectedBucket = ref('')

const showCreateBucket = ref(false)
const newBucketName = ref('')
const creatingBucket = ref(false)
const deletingBucket = ref('')

async function loadBuckets() {
  bucketsLoading.value = true
  try {
    buckets.value = await listBucketsApi(true)
  } catch (e: any) {
    message.error(e.message || '加载存储桶列表失败')
  } finally {
    bucketsLoading.value = false
  }
}

async function selectBucket(name: string) {
  if (selectedBucket.value === name) return
  selectedBucket.value = name
  currentPrefix.value = ''
  resetPagination()
  await fetchEntries()
}

async function handleSubmitCreateBucket() {
  const name = newBucketName.value.trim()
  if (!/^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$/.test(name)) {
    message.warning('桶名只能包含小写字母、数字和中划线，长度 3-63 位')
    return
  }
  creatingBucket.value = true
  try {
    await createBucketApi(name)
    message.success(`存储桶 ${name} 创建成功`)
    showCreateBucket.value = false
    await loadBuckets()
    await selectBucket(name)
  } catch (e: any) {
    message.error(e.message || '创建失败')
  } finally {
    creatingBucket.value = false
  }
}

function afterBucketDeleted(name: string) {
  if (selectedBucket.value === name) {
    selectedBucket.value = ''
    folders.value = []
    files.value = []
  }
  loadBuckets()
}

async function doDeleteBucket(name: string, recursive: boolean) {
  deletingBucket.value = name
  try {
    await deleteBucketApi(name, recursive)
    message.success(`存储桶 ${name} 已删除`)
    afterBucketDeleted(name)
  } catch (e: any) {
    message.error(e.message || '删除失败')
  } finally {
    deletingBucket.value = ''
  }
}

async function handleDeleteBucket(bucket: BucketSummary) {
  deletingBucket.value = bucket.name
  try {
    await deleteBucketApi(bucket.name, false)
    message.success(`存储桶 ${bucket.name} 已删除`)
    afterBucketDeleted(bucket.name)
  } catch (e: any) {
    const msg = String(e?.message || '')
    if (msg.includes('非空') || msg.includes('not empty') || msg.includes('409')) {
      dialog.warning({
        title: '存储桶非空',
        content: `存储桶 ${bucket.name} 内仍有对象。是否递归删除全部对象并删除桶？此操作不可恢复。`,
        positiveText: '递归删除',
        negativeText: '取消',
        onPositiveClick: () => doDeleteBucket(bucket.name, true)
      })
    } else {
      message.error(msg || '删除失败')
    }
  } finally {
    if (deletingBucket.value === bucket.name) {
      deletingBucket.value = ''
    }
  }
}

// ==================== 路径 / 文件列表 ====================
const currentPrefix = ref('')
const folders = ref<FolderEntry[]>([])
const files = ref<FileEntry[]>([])
const loading = ref(false)

// ==================== 分页（基于 nextMarker） ====================
const pageSize = ref(200)
const pageSizeOptions = [50, 100, 200, 500]
const pageSizeSelectOptions = pageSizeOptions.map(v => ({ label: `${v} 条/页`, value: v }))
const nextMarkers = ref<string[]>([])
const currentPage = ref(1)
const hasNextPage = ref(false)
const isTruncated = ref(false)

// ==================== 视图模式 ====================
const VIEW_MODE_KEY = 'runtime-storage-view'
const viewMode = ref<'list' | 'grid'>(
  localStorage.getItem(VIEW_MODE_KEY) === 'grid' ? 'grid' : 'list'
)

function setViewMode(mode: 'list' | 'grid') {
  viewMode.value = mode
  try {
    localStorage.setItem(VIEW_MODE_KEY, mode)
  } catch {
    /* ignore */
  }
}

// ==================== 上传 ====================
const uploading = ref(false)
const uploadProgress = ref(0)
const uploadFileName = ref('')
const fileInputRef = ref<HTMLInputElement | null>(null)

// ==================== 新建目录 ====================
const showCreateFolder = ref(false)
const newFolderName = ref('')
const creatingFolder = ref(false)

// ==================== 预览 ====================
type PreviewType = 'image' | 'video' | 'audio' | 'pdf' | 'text' | 'none'
const previewShow = ref(false)
const previewLoading = ref(false)
const previewFile = ref<FileEntry | null>(null)
const previewBlobUrl = ref('')
const previewTextContent = ref('')
const previewType = ref<PreviewType>('none')

const breadcrumbs = computed(() => {
  const parts = [{ label: selectedBucket.value || '', prefix: '' }]
  if (currentPrefix.value) {
    const segs = currentPrefix.value.split('/').filter(Boolean)
    let path = ''
    for (const seg of segs) {
      path += `${seg}/`
      parts.push({ label: seg, prefix: path })
    }
  }
  return parts
})

const allEntries = computed(() => [
  ...folders.value.map(f => ({ ...f, _key: `folder:${f.prefix}` })),
  ...files.value.map(f => ({ ...f, _key: `file:${f.key}` }))
])

function resetPagination() {
  currentPage.value = 1
  nextMarkers.value = []
  hasNextPage.value = false
  isTruncated.value = false
}

async function fetchEntries(targetPage = currentPage.value) {
  if (!selectedBucket.value) return
  const marker = targetPage <= 1 ? undefined : nextMarkers.value[targetPage - 2]
  if (targetPage > 1 && marker == null) return
  loading.value = true
  try {
    const result = await listObjectsApi(
      selectedBucket.value,
      currentPrefix.value,
      marker,
      pageSize.value
    )
    const parsed = parseEntries(result.objects, currentPrefix.value)
    folders.value = parsed.folders
    files.value = parsed.files
    currentPage.value = targetPage
    hasNextPage.value = !!result.nextMarker && result.truncated
    isTruncated.value = result.truncated
    if (result.nextMarker && result.truncated) {
      nextMarkers.value[targetPage - 1] = result.nextMarker
    } else {
      nextMarkers.value.length = Math.min(nextMarkers.value.length, targetPage - 1)
    }
  } catch (e: any) {
    message.error(e.message || '加载文件列表失败')
    folders.value = []
    files.value = []
  } finally {
    loading.value = false
  }
}

function gotoPage(page: number) {
  if (page < 1 || loading.value) return
  if (page > currentPage.value && !hasNextPage.value) return
  fetchEntries(page)
}

function handlePageSizeChange() {
  resetPagination()
  fetchEntries()
}

function refreshAll() {
  resetPagination()
  fetchEntries()
  loadBuckets()
}

function navigateTo(prefix: string) {
  currentPrefix.value = prefix
  resetPagination()
  fetchEntries()
}

function goUp() {
  if (!currentPrefix.value) return
  const segs = currentPrefix.value.split('/').filter(Boolean)
  segs.pop()
  navigateTo(segs.length ? `${segs.join('/')}/` : '')
}

function handleFolderOpen(folder: FolderEntry) {
  navigateTo(folder.prefix)
}

function handleBreadcrumb(idx: number) {
  navigateTo(breadcrumbs.value[idx]!.prefix)
}

function handleCreateFolder() {
  newFolderName.value = ''
  showCreateFolder.value = true
}

async function handleSubmitCreateFolder() {
  const name = newFolderName.value.trim()
  if (!name) {
    message.warning('请输入目录名称')
    return
  }
  if (name.includes('/')) {
    message.warning('目录名称不能包含 /')
    return
  }
  creatingFolder.value = true
  try {
    await createFolderApi(selectedBucket.value, currentPrefix.value, name)
    message.success('目录创建成功')
    showCreateFolder.value = false
    resetPagination()
    await fetchEntries()
  } catch (e: any) {
    message.error(e.message || '创建失败')
  } finally {
    creatingFolder.value = false
  }
}

function handleUploadClick() {
  fileInputRef.value?.click()
}

async function handleFileChange(e: Event) {
  const input = e.target as HTMLInputElement
  const fileList = input.files
  if (!fileList || fileList.length === 0) return
  const filesToUpload = Array.from(fileList)
  input.value = ''
  uploading.value = true
  uploadProgress.value = 0
  try {
    for (let i = 0; i < filesToUpload.length; i++) {
      const file = filesToUpload[i]!
      uploadFileName.value = filesToUpload.length > 1
        ? `${file.name} (${i + 1}/${filesToUpload.length})`
        : file.name
      uploadProgress.value = 0
      const key = currentPrefix.value + file.name
      const contentType = file.type || 'application/octet-stream'
      await uploadObjectApi(
        selectedBucket.value,
        key,
        file,
        contentType,
        p => { uploadProgress.value = p }
      )
    }
    message.success(filesToUpload.length > 1 ? `${filesToUpload.length} 个文件上传成功` : '上传成功')
    resetPagination()
    await fetchEntries()
  } catch (e: any) {
    message.error(e.message || '上传失败')
  } finally {
    uploading.value = false
    uploadProgress.value = 0
    uploadFileName.value = ''
  }
}

async function handleDownload(file: FileEntry) {
  try {
    const blob = await downloadObjectApi(selectedBucket.value, file.key)
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = file.name
    document.body.appendChild(a)
    a.click()
    document.body.removeChild(a)
    URL.revokeObjectURL(url)
  } catch (e: any) {
    message.error(e.message || '下载失败')
  }
}

async function handleDeleteFile(file: FileEntry) {
  try {
    await deleteObjectApi(selectedBucket.value, file.key)
    message.success('删除成功')
    resetPagination()
    await fetchEntries()
  } catch (e: any) {
    message.error(e.message || '删除失败')
  }
}

async function handleDeleteFolder(folder: FolderEntry) {
  try {
    await deleteFolderApi(selectedBucket.value, folder.prefix)
    message.success('文件夹已删除')
    resetPagination()
    await fetchEntries()
  } catch (e: any) {
    message.error(e.message || '删除失败')
  }
}

async function handlePreview(file: FileEntry) {
  if (!isPreviewable(file.name, file.contentType)) {
    message.info('该文件类型暂不支持在线预览，请下载后查看')
    return
  }
  previewFile.value = file
  previewShow.value = true
  previewBlobUrl.value = ''
  previewTextContent.value = ''
  previewType.value = 'none'
  const category = getFileCategory(file.name, file.contentType)
  if (category === 'text' || category === 'code') {
    // 文本需全文渲染且通常很小，保持整块下载 + loading
    previewLoading.value = true
    try {
      previewTextContent.value = await getTextContentApi(selectedBucket.value, file.key)
      previewType.value = 'text'
    } catch (e: any) {
      message.error(e.message || '预览失败')
    } finally {
      previewLoading.value = false
    }
    return
  }
  // 媒体直连流式 URL：浏览器自行 Range 请求，边下边播（缓冲进度由播放器自带 UI 呈现）
  previewBlobUrl.value = getObjectStreamUrl(selectedBucket.value, file.key)
  if (category === 'image') previewType.value = 'image'
  else if (category === 'video') previewType.value = 'video'
  else if (category === 'audio') previewType.value = 'audio'
  else previewType.value = 'pdf'
}

/** 媒体元素加载失败（直连 URL 无 Promise 可捕获，经 error 事件提示）。 */
function onPreviewMediaError() {
  if (!previewShow.value) return // 关闭过程中的卸载事件不提示
  message.error('预览加载失败，请重试或下载后查看')
}

function closePreview() {
  previewShow.value = false
  if (previewBlobUrl.value) {
    // 仅 blob: URL 需释放；直连流式 URL 无需 revoke
    if (previewBlobUrl.value.startsWith('blob:')) {
      URL.revokeObjectURL(previewBlobUrl.value)
    }
    previewBlobUrl.value = ''
  }
  previewTextContent.value = ''
  previewType.value = 'none'
  previewFile.value = null
  previewLoading.value = false
}

/** 图标按钮（带悬浮提示），用于表格行操作 */
function renderIconBtn(
  icon: string,
  tip: string,
  onClick: () => void,
  type: 'default' | 'primary' | 'error' = 'default'
) {
  return h(NTooltip, { trigger: 'hover' }, {
    trigger: () => h(
      NButton,
      { size: 'tiny', quaternary: true, type, onClick },
      { icon: () => h(ActionIcon, { name: icon, size: 14 }) }
    ),
    default: () => tip
  })
}

const columns = computed<DataTableColumns<any>>(() => [
  {
    title: '名称',
    key: 'name',
    minWidth: 220,
    render(row: any) {
      const isFolder = row.type === 'folder'
      return h(
        'div',
        {
          class: 'entry-name-cell',
          onClick: () => isFolder && handleFolderOpen(row as FolderEntry)
        },
        [
          h(FileIcon, {
            name: row.name,
            contentType: row.contentType,
            size: 18,
            folder: isFolder
          }),
          h('span', { class: 'entry-name-text', title: row.name }, row.name)
        ]
      )
    }
  },
  {
    title: '大小',
    key: 'size',
    width: 100,
    render(row: any) {
      if (row.type === 'folder') return '-'
      return formatFileSize((row as FileEntry).size)
    }
  },
  {
    title: '类型',
    key: 'type',
    width: 90,
    render(row: any) {
      if (row.type === 'folder') return '文件夹'
      const cat = getFileCategory(row.name, row.contentType)
      const typeMap: Record<string, string> = {
        image: '图片', video: '视频', audio: '音频', text: '文本',
        pdf: 'PDF', archive: '压缩包', code: '代码', file: '文件'
      }
      return typeMap[cat] || '文件'
    }
  },
  {
    title: '修改时间',
    key: 'createdAt',
    width: 150,
    render(row: any) {
      if (row.type === 'folder') return '-'
      return formatDate((row as FileEntry).createdAt)
    }
  },
  {
    title: '操作',
    key: 'actions',
    width: 130,
    render(row: any) {
      if (row.type === 'folder') {
        const folder = row as FolderEntry
        return h('div', { class: 'entry-actions' }, [
          renderIconBtn('folder-plus', '打开', () => handleFolderOpen(folder), 'primary'),
          h(NPopconfirm, { onPositiveClick: () => handleDeleteFolder(folder) }, {
            trigger: () => renderIconBtn('trash', '删除', () => {}, 'error'),
            default: () => `确定删除文件夹 ${folder.name} 及其所有内容吗？`
          })
        ])
      }
      const file = row as FileEntry
      const children: any[] = []
      if (isPreviewable(file.name, file.contentType)) {
        children.push(renderIconBtn('eye', '预览', () => handlePreview(file)))
      }
      children.push(renderIconBtn('download', '下载', () => handleDownload(file), 'primary'))
      children.push(h(NPopconfirm, { onPositiveClick: () => handleDeleteFile(file) }, {
        trigger: () => renderIconBtn('trash', '删除', () => {}, 'error'),
        default: () => `确定删除文件 ${file.name} 吗？`
      }))
      return h('div', { class: 'entry-actions' }, children)
    }
  }
])

const rowProps = (row: any) => ({
  style: 'cursor: default;',
  onDblclick: () => {
    if (row.type === 'folder') handleFolderOpen(row as FolderEntry)
    else handlePreview(row as FileEntry)
  }
})

onMounted(async () => {
  await loadBuckets()
  if (buckets.value.length > 0) {
    await selectBucket(buckets.value[0]!.name)
  }
})
</script>

<template>
  <div class="storage-page">
    <!-- 左侧：存储桶面板 -->
    <aside class="bucket-panel">
      <div class="bucket-panel-header">
        <span class="panel-title">存储桶</span>
        <div class="panel-actions">
          <NTooltip trigger="hover">
            <template #trigger>
              <NButton size="tiny" quaternary @click="showCreateBucket = true">
                <template #icon><ActionIcon name="plus" :size="14" /></template>
              </NButton>
            </template>
            新建存储桶
          </NTooltip>
          <NTooltip trigger="hover">
            <template #trigger>
              <NButton size="tiny" quaternary :loading="bucketsLoading" @click="loadBuckets">
                <template #icon><ActionIcon name="refresh" :size="14" /></template>
              </NButton>
            </template>
            刷新桶列表
          </NTooltip>
        </div>
      </div>

      <div class="bucket-list">
        <NSpin :show="bucketsLoading">
          <div
            v-for="b in buckets"
            :key="b.name"
            class="bucket-item"
            :class="{ active: b.name === selectedBucket }"
            @click="selectBucket(b.name)"
          >
            <div class="bucket-item-main">
              <div class="bucket-item-name" :title="b.name">{{ b.name }}</div>
              <div class="bucket-item-stats">
                <span v-if="b.objectCount != null">
                  {{ b.objectCountEstimate ? '≈' : '' }}{{ b.objectCount }} 个对象
                </span>
                <span v-if="b.totalSize != null">{{ formatFileSize(b.totalSize) }}</span>
              </div>
            </div>
            <NPopconfirm
              :show-icon="false"
              @positive-click="handleDeleteBucket(b)"
            >
              <template #trigger>
                <NButton
                  class="bucket-item-delete"
                  size="tiny"
                  quaternary
                  type="error"
                  :loading="deletingBucket === b.name"
                  @click.stop
                >
                  <template #icon><ActionIcon name="trash" :size="13" /></template>
                </NButton>
              </template>
              确定删除存储桶 {{ b.name }} 吗？
            </NPopconfirm>
          </div>
          <NEmpty
            v-if="!bucketsLoading && buckets.length === 0"
            description="暂无存储桶"
            size="small"
            :style="{ padding: '40px 0' }"
          />
        </NSpin>
      </div>
    </aside>

    <!-- 右侧：文件浏览器 -->
    <section class="browser-panel">
      <template v-if="selectedBucket">
        <!-- 工具栏 -->
        <div class="browser-toolbar">
          
          <div class="toolbar-group">
          
            <template v-for="(crumb, idx) in breadcrumbs" :key="idx">
            <span
              class="breadcrumb-item"
              :class="{ clickable: idx < breadcrumbs.length - 1 }"
              :title="crumb.prefix || `根目录（${selectedBucket}）`"
              @click="idx < breadcrumbs.length - 1 && handleBreadcrumb(idx)"
            >
              {{ crumb.label }}
            </span>
            <span v-if="idx < breadcrumbs.length - 1" class="breadcrumb-sep">/</span>
          </template>
          </div>
          <div class="toolbar-group">
            <NButton size="small" type="primary" :disabled="uploading" @click="handleUploadClick">
              <template #icon><ActionIcon name="upload" :size="14" /></template>
              上传
            </NButton>
            <NButton size="small" :disabled="uploading" @click="handleCreateFolder">
              <template #icon><ActionIcon name="folder-plus" :size="14" /></template>
              新建目录
            </NButton>
            <NButtonGroup size="small">
              <NTooltip trigger="hover">
                <template #trigger>
                  <NButton
                    :type="viewMode === 'list' ? 'primary' : 'default'"
                    :quaternary="viewMode !== 'list'"
                    @click="setViewMode('list')"
                  >
                    <template #icon><ActionIcon name="list" :size="14" /></template>
                  </NButton>
                </template>
                列表视图
              </NTooltip>
              <NTooltip trigger="hover">
                <template #trigger>
                  <NButton
                    :type="viewMode === 'grid' ? 'primary' : 'default'"
                    :quaternary="viewMode !== 'grid'"
                    @click="setViewMode('grid')"
                  >
                    <template #icon><ActionIcon name="grid" :size="14" /></template>
                  </NButton>
                </template>
                矩阵视图
              </NTooltip>
            </NButtonGroup>
            <NTooltip trigger="hover">
              <template #trigger>
                <NButton size="small" quaternary :loading="loading" @click="refreshAll">
                  <template #icon><ActionIcon name="refresh" :size="14" /></template>
                </NButton>
              </template>
              刷新
            </NTooltip>
          </div>
          <input
            ref="fileInputRef"
            type="file"
            multiple
            class="hidden-file-input"
            @change="handleFileChange"
          />
        </div>
        <!-- 上传进度 -->
        <div v-if="uploading" class="upload-progress-bar">
          <div class="upload-info">
            <span class="upload-name" :title="uploadFileName">{{ uploadFileName }}</span>
            <span class="upload-percent">{{ uploadProgress }}%</span>
          </div>
          <NProgress
            type="line"
            :percentage="uploadProgress"
            :show-indicator="false"
            :height="4"
            status="info"
          />
        </div>

        <!-- 文件内容区（可滚动） -->
        <div class="file-content">
          <NSpin :show="loading" :style="{ width: '100%' }">
            <template v-if="!loading && allEntries.length === 0">
              <NEmpty description="此目录为空" :style="{ padding: '80px 0' }" />
            </template>
            <template v-else-if="viewMode === 'list'">
              <NDataTable
                :columns="columns"
                :data="allEntries"
                :row-key="(row: any) => row._key"
                :row-props="rowProps"
                :single-line="false"
                size="small"
                :bordered="false"
                :scroll-x="690"
              />
            </template>
            <template v-else>
              <div class="grid-view">
                <div
                  v-for="folder in folders"
                  :key="`folder:${folder.prefix}`"
                  class="grid-tile"
                  :title="folder.name"
                  @click="handleFolderOpen(folder)"
                >
                  <FileIcon folder size="40" class="grid-tile-icon" />
                  <span class="grid-tile-name">{{ folder.name }}</span>
                  <div class="grid-tile-actions" @click.stop @dblclick.stop>
                    <NPopconfirm :show-icon="false" @positive-click="handleDeleteFolder(folder)">
                      <template #trigger>
                        <NButton size="tiny" quaternary type="error">
                          <template #icon><ActionIcon name="trash" :size="13" /></template>
                        </NButton>
                      </template>
                      确定删除文件夹 {{ folder.name }} 及其所有内容吗？
                    </NPopconfirm>
                  </div>
                </div>
                <div
                  v-for="file in files"
                  :key="`file:${file.key}`"
                  class="grid-tile"
                  :title="file.name"
                  @dblclick="handlePreview(file)"
                >
                  <FileIcon :name="file.name" :content-type="file.contentType" size="40" class="grid-tile-icon" />
                  <span class="grid-tile-name">{{ file.name }}</span>
                  <div class="grid-tile-actions" @click.stop @dblclick.stop>
                    <NButton
                      v-if="isPreviewable(file.name, file.contentType)"
                      size="tiny"
                      quaternary
                      @click="handlePreview(file)"
                    >
                      <template #icon><ActionIcon name="eye" :size="13" /></template>
                    </NButton>
                    <NButton size="tiny" quaternary @click="handleDownload(file)">
                      <template #icon><ActionIcon name="download" :size="13" /></template>
                    </NButton>
                    <NPopconfirm :show-icon="false" @positive-click="handleDeleteFile(file)">
                      <template #trigger>
                        <NButton size="tiny" quaternary type="error">
                          <template #icon><ActionIcon name="trash" :size="13" /></template>
                        </NButton>
                      </template>
                      确定删除文件 {{ file.name }} 吗？
                    </NPopconfirm>
                  </div>
                </div>
              </div>
            </template>
          </NSpin>
        </div>

        <!-- 分页栏 -->
        <div v-if="allEntries.length > 0" class="pagination-bar">
          <div class="pagination-info">
            本页 {{ allEntries.length }} 项（{{ folders.length }} 个目录 / {{ files.length }} 个文件）
            <template v-if="isTruncated && !hasNextPage">（结果被截断）</template>
          </div>
          <div class="pagination-controls">
            <NButton size="small" :disabled="currentPage <= 1 || loading" @click="gotoPage(currentPage - 1)">
              上一页
            </NButton>
            <span class="page-indicator">第 {{ currentPage }} 页</span>
            <NButton size="small" :disabled="!hasNextPage || loading" @click="gotoPage(currentPage + 1)">
              下一页
            </NButton>
            <NSelect
              v-model:value="pageSize"
              size="small"
              :options="pageSizeSelectOptions"
              :style="{ width: '104px' }"
              @update:value="handlePageSizeChange"
            />
          </div>
        </div>
      </template>

      <NEmpty
        v-else
        :description="buckets.length === 0 ? '暂无存储桶，请先在左侧创建' : '从左侧选择一个存储桶'"
        :style="{ margin: 'auto' }"
      />
    </section>

    <!-- 新建存储桶 -->
    <WwModal
      v-model:show="showCreateBucket"
      title="新建存储桶"
      :width="440"
      @close="newBucketName = ''"
    >
      <div>
        <NInput
          v-model:value="newBucketName"
          placeholder="请输入桶名（3-63 位小写字母/数字/中划线）"
          @keyup.enter="handleSubmitCreateBucket"
        />
        <div class="form-hint">
          桶名需以字母或数字开头和结尾，仅含小写字母、数字与中划线
        </div>
      </div>
      <template #footer>
        <div class="modal-footer">
          <NButton @click="showCreateBucket = false">取消</NButton>
          <NButton type="primary" :loading="creatingBucket" @click="handleSubmitCreateBucket">
            确定
          </NButton>
        </div>
      </template>
    </WwModal>

    <!-- 新建目录 -->
    <WwModal
      v-model:show="showCreateFolder"
      title="新建目录"
      :width="440"
      @close="newFolderName = ''"
    >
      <div>
        <NInput
          v-model:value="newFolderName"
          placeholder="请输入目录名称"
          @keyup.enter="handleSubmitCreateFolder"
        />
        <div class="form-hint">
          目录将在当前路径下创建，名称不能包含 /
        </div>
      </div>
      <template #footer>
        <div class="modal-footer">
          <NButton @click="showCreateFolder = false">取消</NButton>
          <NButton type="primary" :loading="creatingFolder" @click="handleSubmitCreateFolder">
            确定
          </NButton>
        </div>
      </template>
    </WwModal>

    <!-- 文件预览 -->
    <WwModal
      v-model:show="previewShow"
      :title="previewFile?.name"
      :width="860"
      :resizable="true"
      @close="closePreview"
    >
      <div class="preview-container">
        <NSpin :show="previewLoading" :style="{ width: '100%' }">
          <template v-if="previewType === 'image'">
            <img
              :src="previewBlobUrl"
              :alt="previewFile?.name"
              class="preview-image"
              @error="onPreviewMediaError"
            />
          </template>
          <template v-else-if="previewType === 'video'">
            <video :src="previewBlobUrl" controls class="preview-video" @error="onPreviewMediaError" />
          </template>
          <template v-else-if="previewType === 'audio'">
            <div class="preview-audio-wrap">
              <FileIcon
                v-if="previewFile"
                :name="previewFile.name"
                :content-type="previewFile.contentType"
                :size="56"
              />
              <audio :src="previewBlobUrl" controls class="preview-audio" @error="onPreviewMediaError" />
            </div>
          </template>
          <template v-else-if="previewType === 'pdf'">
            <iframe :src="previewBlobUrl" class="preview-pdf" />
          </template>
          <template v-else-if="previewType === 'text'">
            <pre class="preview-text">{{ previewTextContent }}</pre>
          </template>
          <template v-else>
            <div class="preview-placeholder" />
          </template>
        </NSpin>
      </div>
      <template #footer>
        <div class="modal-footer">
          <NButton v-if="previewFile" type="primary" @click="handleDownload(previewFile)">
            <template #icon><ActionIcon name="download" :size="14" /></template>
            下载
          </NButton>
          <NButton @click="previewShow = false">关闭</NButton>
        </div>
      </template>
    </WwModal>
  </div>
</template>

<style scoped>
.storage-page {
  display: flex;
  height: 100%;
  min-height: 0;
  background: var(--ww-surface);
  border: 1px solid var(--ww-border);
  border-radius: var(--ww-radius-lg);
  overflow: hidden;
}

/* ==================== 左侧桶面板 ==================== */
.bucket-panel {
  width: 248px;
  flex-shrink: 0;
  display: flex;
  flex-direction: column;
  border-right: 1px solid var(--ww-border);
  min-height: 0;
}

.bucket-panel-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 12px 12px 12px 16px;
  border-bottom: 1px solid var(--ww-border);
  flex-shrink: 0;
}

.panel-title {
  font-size: 13px;
  font-weight: 600;
  color: var(--ww-text-1);
}

.panel-actions {
  display: flex;
  align-items: center;
  gap: 2px;
}

.bucket-list {
  flex: 1;
  min-height: 0;
  overflow-y: auto;
  padding: 0px 6px;
}

.bucket-item {
  position: relative;
  display: flex;
  align-items: center;
  gap: 4px;
  padding: 9px 8px 9px 12px;
  margin-top: 4px;
  border-radius: var(--ww-radius);
  cursor: pointer;
  transition: background-color 0.15s ease;
}

.bucket-item:hover {
  background-color: var(--ww-surface-hover);
}

.bucket-item.active {
  background-color: var(--ww-primary-soft);
}

.bucket-item.active::before {
  content: '';
  position: absolute;
  left: 2px;
  top: 50%;
  transform: translateY(-50%);
  width: 3px;
  height: 18px;
  border-radius: 2px;
  background: var(--ww-primary);
}

.bucket-item-main {
  flex: 1;
  min-width: 0;
}

.bucket-item-name {
  font-size: 13px;
  font-weight: 500;
  color: var(--ww-text-1);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.bucket-item.active .bucket-item-name {
  color: var(--ww-primary);
  font-weight: 600;
}

.bucket-item-stats {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-top: 2px;
  font-size: 11px;
  color: var(--ww-text-3);
  overflow: hidden;
  white-space: nowrap;
}

.bucket-item-delete {
  opacity: 0;
  transition: opacity 0.15s ease;
}

.bucket-item:hover .bucket-item-delete {
  opacity: 1;
}

/* ==================== 右侧文件浏览器 ==================== */
.browser-panel {
  flex: 1;
  min-width: 0;
  min-height: 0;
  display: flex;
  flex-direction: column;
}

.browser-toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 10px 14px;
  border-bottom: 1px solid var(--ww-border);
  flex-shrink: 0;
  flex-wrap: wrap;
}

.toolbar-group {
  display: flex;
  align-items: center;
  gap: 8px;
}

.hidden-file-input {
  position: absolute;
  width: 0;
  height: 0;
  opacity: 0;
  overflow: hidden;
  pointer-events: none;
}

.breadcrumb-bar {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 2px;
  padding: 8px 16px;
  border-bottom: 1px solid var(--ww-border);
  background-color: var(--ww-surface-2);
  flex-shrink: 0;
  overflow-x: auto;
}

.breadcrumb-item {
  font-size: 13px;
  color: var(--ww-text-1);
  font-weight: 500;
  padding: 2px 6px;
  border-radius: 4px;
  white-space: nowrap;
}

.breadcrumb-item.clickable {
  cursor: pointer;
  color: var(--ww-primary);
}

.breadcrumb-item.clickable:hover {
  background-color: var(--ww-surface-hover);
}

.breadcrumb-sep {
  font-size: 13px;
  color: var(--ww-text-3);
}

.upload-progress-bar {
  padding: 10px 16px;
  border-bottom: 1px solid var(--ww-border);
  background-color: var(--ww-surface-2);
  flex-shrink: 0;
}

.upload-info {
  display: flex;
  justify-content: space-between;
  margin-bottom: 4px;
  font-size: 12px;
}

.upload-name {
  color: var(--ww-text-2);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  max-width: 80%;
}

.upload-percent {
  color: var(--ww-primary);
  font-weight: 600;
}

.file-content {
  flex: 1;
  min-height: 0;
  overflow: auto;
}

/* ==================== 列表视图 ==================== */
.entry-name-cell {
  display: flex;
  align-items: center;
  gap: 8px;
  min-width: 0;
  cursor: default;
}

.entry-name-text {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.entry-actions {
  display: flex;
  align-items: center;
  gap: 2px;
  opacity: 0;
  transition: opacity 0.15s ease;
}

:deep(.n-data-table-tr:hover) .entry-actions {
  opacity: 1;
}

/* ==================== 矩阵视图 ==================== */
.grid-view {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(112px, 1fr));
  gap: 8px;
  padding: 14px;
}

.grid-tile {
  position: relative;
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 8px;
  padding: 16px 8px 12px;
  border-radius: var(--ww-radius);
  cursor: default;
  border: 1px solid var(--ww-border);
  background: var(--ww-surface);
  transition: background-color 0.15s ease, border-color 0.15s ease, transform 0.15s ease;
  user-select: none;
}

.grid-tile:hover {
  background-color: var(--ww-surface-hover);
  border-color: var(--ww-border-strong);
  transform: translateY(-1px);
}

.grid-tile-icon {
  display: block;
}

.grid-tile-name {
  font-size: 12px;
  color: var(--ww-text-1);
  text-align: center;
  max-width: 100%;
  display: -webkit-box;
  -webkit-line-clamp: 2;
  -webkit-box-orient: vertical;
  overflow: hidden;
  word-break: break-all;
  line-height: 1.4;
}

.grid-tile-actions {
  position: absolute;
  top: 4px;
  right: 4px;
  display: flex;
  gap: 0;
  opacity: 0;
  transition: opacity 0.15s ease;
  background-color: var(--ww-surface-2);
  border: 1px solid var(--ww-border);
  border-radius: var(--ww-radius-sm);
  box-shadow: var(--ww-shadow-float);
}

.grid-tile:hover .grid-tile-actions {
  opacity: 1;
}

/* ==================== 分页栏 ==================== */
.pagination-bar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 10px 16px;
  border-top: 1px solid var(--ww-border);
  background: var(--ww-surface-2);
  flex-shrink: 0;
  flex-wrap: wrap;
}

.pagination-info {
  font-size: 12px;
  color: var(--ww-text-3);
}

.pagination-controls {
  display: flex;
  align-items: center;
  gap: 8px;
}

.page-indicator {
  font-size: 13px;
  min-width: 64px;
  text-align: center;
  color: var(--ww-text-2);
}

/* ==================== 模态框 ==================== */
.form-hint {
  margin-top: 8px;
  font-size: 12px;
  line-height: 1.6;
  color: var(--ww-text-3);
}

.modal-footer {
  display: flex;
  justify-content: flex-end;
  gap: 8px;
}

/* ==================== 预览 ==================== */
.preview-container {
  max-height: 68vh;
  overflow: auto;
}

.preview-image {
  display: block;
  max-width: 100%;
  max-height: 64vh;
  margin: 0 auto;
}

.preview-video {
  display: block;
  width: 100%;
  max-height: 64vh;
}

.preview-audio-wrap {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 20px;
  padding: 30px 0;
}

.preview-audio {
  width: 100%;
  max-width: 560px;
}

.preview-pdf {
  width: 100%;
  height: 66vh;
  border: 0;
}

.preview-text {
  background-color: var(--ww-surface-2);
  border: 1px solid var(--ww-border);
  border-radius: var(--ww-radius-sm);
  color: var(--ww-text-1);
  padding: 12px 14px;
  font-size: 13px;
  line-height: 1.6;
  white-space: pre-wrap;
  word-break: break-word;
  font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace;
  margin: 0;
  max-height: 64vh;
  overflow: auto;
}

.preview-placeholder {
  min-height: 220px;
}
</style>
