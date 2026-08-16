<script setup lang="ts">
import {
  computed,
  defineComponent,
  h,
  ref,
  watch,
} from 'vue';

import {
  ChevronRight,
  createIconifyIcon,
  Download,
  Eye,
  Grid,
  List,
  Plus,
  RotateCw,
  Upload,
} from '@vben/icons';

import {
  type DataTableColumns,
  NButton,
  NCard,
  NDataTable,
  NEmpty,
  NIcon,
  NInput,
  NPopconfirm,
  NProgress,
  NSelect,
  NSpace,
  NSpin,
  NTooltip,
} from 'naive-ui';

import { message } from '#/adapter/naive';
import {
  createBucketApi,
  createFolderApi,
  deleteBucketApi,
  deleteFolderApi,
  deleteObjectApi,
  downloadObjectApi,
  type FileEntry,
  type FolderEntry,
  formatDate,
  formatFileSize,
  getBucketsApi,
  getFileCategory,
  listEntriesApi,
  type StorageBucket,
  uploadFileApi,
} from '#/api/core/storage';
import FilePreview from '#/components/FilePreview/index.vue';
import { WwModal } from '#/components/WwModal';

defineOptions({ name: 'SystemStorage' });

// ==================== 图标组件 ====================
const FolderIcon = createIconifyIcon('lucide:folder');
const BucketIcon = createIconifyIcon('lucide:database');
const ImageFileIcon = createIconifyIcon('lucide:image');
const VideoFileIcon = createIconifyIcon('lucide:film');
const AudioFileIcon = createIconifyIcon('lucide:music');
const TextFileIcon = createIconifyIcon('lucide:file-text');
const PdfFileIcon = createIconifyIcon('lucide:file-text');
const ArchiveFileIcon = createIconifyIcon('lucide:archive');
const CodeFileIcon = createIconifyIcon('lucide:file-code');
const GenericFileIcon = createIconifyIcon('lucide:file');

const FILE_ICON_MAP: Record<string, ReturnType<typeof createIconifyIcon>> = {
  image: ImageFileIcon,
  video: VideoFileIcon,
  audio: AudioFileIcon,
  text: TextFileIcon,
  pdf: PdfFileIcon,
  archive: ArchiveFileIcon,
  code: CodeFileIcon,
  file: GenericFileIcon,
};

const FILE_COLOR_MAP: Record<string, string> = {
  folder: '#f0b429',
  image: '#10b981',
  video: '#8b5cf6',
  audio: '#f59e0b',
  text: '#3b82f6',
  pdf: '#ef4444',
  archive: '#6b7280',
  code: '#06b6d4',
  file: '#9ca3af',
};

const FileTypeIcon = defineComponent({
  name: 'FileTypeIcon',
  props: {
    category: { type: String, default: 'file' },
    size: { type: Number, default: 20 },
  },
  setup(props) {
    return () => {
      const category = props.category || 'file';
      const isFolder = category === 'folder';
      const IconComp = isFolder ? FolderIcon : (FILE_ICON_MAP[category] ?? GenericFileIcon);
      return h(IconComp, {
        width: props.size,
        height: props.size,
        color: FILE_COLOR_MAP[category] ?? FILE_COLOR_MAP.file,
      });
    };
  },
});

// ==================== 状态 ====================
const buckets = ref<StorageBucket[]>([]);
const selectedBucket = ref<string>('');
const currentPrefix = ref('');
const folders = ref<FolderEntry[]>([]);
const files = ref<FileEntry[]>([]);
const loading = ref(false);
const viewMode = ref<'grid' | 'list'>('list');
const showCreateBucket = ref(false);
const newBucketName = ref('');
const creatingBucket = ref(false);

// 新建目录
const showCreateFolder = ref(false);
const newFolderName = ref('');
const creatingFolder = ref(false);

// 文件上传
const uploading = ref(false);
const uploadProgress = ref(0);
const uploadFileName = ref('');
const fileInputRef = ref<HTMLInputElement | null>(null);

// 文件预览
const previewShow = ref(false);
const previewBucket = ref('');
const previewFileKey = ref('');
const previewFileName = ref('');
const previewFileSize = ref(0);

// 分页（服务端 marker 游标分页：每次只加载一页，避免超大数据量一次性加载导致前后端崩溃）
const currentPage = ref(1);
const pageSize = ref(50);
const pageSizeOptions = [20, 50, 100, 200];
const pageSizeSelectOptions = pageSizeOptions.map((v) => ({
  label: `${v} 条/页`,
  value: v,
}));
/** nextMarkers[i] = 第 i+1 页返回的游标，用于请求第 i+2 页 */
const nextMarkers = ref<string[]>([]);
const hasNextPage = ref(false);
/** 桶内对象数超出后端索引上限时为 true */
const indexTruncated = ref(false);

// ==================== 计算属性 ====================
const currentBucket = computed(() =>
  buckets.value.find((b) => b.name === selectedBucket.value),
);

const breadcrumbs = computed(() => {
  const parts = [{ label: currentBucket.value?.name || '', prefix: '' }];
  if (currentPrefix.value) {
    const segs = currentPrefix.value.split('/').filter(Boolean);
    let path = '';
    for (const seg of segs) {
      path += `${seg}/`;
      parts.push({ label: seg, prefix: path });
    }
  }
  return parts;
});

const allEntries = computed(() => [
  ...folders.value.map((f) => ({ ...f, _key: `folder:${f.prefix}` })),
  ...files.value.map((f) => ({ ...f, _key: `file:${f.key}` })),
]);

// ==================== 数据加载 ====================
async function fetchBuckets(refresh = false) {
  try {
    buckets.value = await getBucketsApi(refresh);
    if (buckets.value.length > 0 && !selectedBucket.value) {
      selectedBucket.value = buckets.value[0]!.name;
    }
  } catch {
    // error handled by interceptor
  }
}

/** 重置分页状态（切换桶/目录、变更页大小时调用） */
function resetPagination() {
  currentPage.value = 1;
  nextMarkers.value = [];
  hasNextPage.value = false;
  indexTruncated.value = false;
}

/**
 * 加载指定页：首页不传游标，后续页使用上一页返回的 nextMarkers。
 * @param refresh 数据变更后强制重建后端快照
 * @param targetPage 目标页码（只能翻到已访问过的页或下一页）
 */
async function fetchEntries(refresh = false, targetPage = currentPage.value) {
  if (!selectedBucket.value) return;
  const marker = targetPage <= 1 ? undefined : nextMarkers.value[targetPage - 2];
  if (targetPage > 1 && marker == null) return;
  loading.value = true;
  try {
    const result = await listEntriesApi(
      selectedBucket.value,
      currentPrefix.value,
      marker,
      pageSize.value,
      refresh,
    );
    folders.value = result.folders;
    files.value = result.files;
    currentPage.value = targetPage;
    hasNextPage.value = result.isTruncated;
    indexTruncated.value = result.indexTruncated === true;
    if (result.isTruncated && result.nextMarker) {
      nextMarkers.value[targetPage - 1] = result.nextMarker;
    } else {
      // 已是末页：丢弃多余的游标
      nextMarkers.value.length = Math.min(nextMarkers.value.length, targetPage - 1);
    }
  } finally {
    loading.value = false;
  }
}

/** 翻页（上一页总是已访问过；下一页的游标在加载当前页时已记录） */
function gotoPage(page: number) {
  if (page < 1 || loading.value) return;
  if (page > currentPage.value && !hasNextPage.value) return;
  void fetchEntries(false, page);
}

function handlePageSizeChange() {
  resetPagination();
  void fetchEntries();
}

async function refreshBucketStats(refresh = false) {
  if (!selectedBucket.value) return;
  try {
    buckets.value = await getBucketsApi(refresh);
  } catch {
    // ignore
  }
}

watch(selectedBucket, () => {
  currentPrefix.value = '';
  resetPagination();
  fetchEntries();
});

// ==================== 存储桶操作 ====================
function handleSelectBucket(name: string) {
  selectedBucket.value = name;
}

function handleCreateBucket() {
  newBucketName.value = '';
  showCreateBucket.value = true;
}

async function handleSubmitCreateBucket() {
  const name = newBucketName.value.trim();
  if (!name) {
    message.warning('请输入存储桶名称');
    return;
  }
  if (!/^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$/.test(name)) {
    message.warning('存储桶名称只能包含小写字母、数字和中划线，长度3-63位');
    return;
  }
  creatingBucket.value = true;
  try {
    await createBucketApi(name);
    message.success('创建成功');
    showCreateBucket.value = false;
    await fetchBuckets(true);
    selectedBucket.value = name;
  } finally {
    creatingBucket.value = false;
  }
}

async function handleDeleteBucket(name: string) {
  await deleteBucketApi(name, true);
  message.success('删除成功');
  if (selectedBucket.value === name) {
    selectedBucket.value = '';
  }
  await fetchBuckets();
  if (buckets.value.length > 0 && !selectedBucket.value) {
    selectedBucket.value = buckets.value[0]!.name;
  } else {
    folders.value = [];
    files.value = [];
  }
}

// ==================== 导航 ====================
function navigateTo(prefix: string) {
  currentPrefix.value = prefix;
  resetPagination();
  fetchEntries();
}

function handleFolderOpen(folder: FolderEntry) {
  navigateTo(folder.prefix);
}

function handleBreadcrumb(idx: number) {
  if (idx === 0) {
    navigateTo('');
  } else {
    navigateTo(breadcrumbs.value[idx]!.prefix);
  }
}

// ==================== 新建目录 ====================
function handleCreateFolder() {
  newFolderName.value = '';
  showCreateFolder.value = true;
}

async function handleSubmitCreateFolder() {
  const name = newFolderName.value.trim();
  if (!name) {
    message.warning('请输入目录名称');
    return false;
  }
  if (name.includes('/')) {
    message.warning('目录名称不能包含 /');
    return false;
  }
  creatingFolder.value = true;
  try {
    await createFolderApi(selectedBucket.value, currentPrefix.value, name);
    message.success('目录创建成功');
    showCreateFolder.value = false;
    resetPagination();
    await fetchEntries(true);
    await refreshBucketStats(true);
  } catch {
    // error handled by interceptor
  } finally {
    creatingFolder.value = false;
  }
  return true;
}

// ==================== 文件上传 ====================
function handleUploadClick() {
  fileInputRef.value?.click();
}

async function handleFileChange(e: Event) {
  const input = e.target as HTMLInputElement;
  const fileList = input.files;
  if (!fileList || fileList.length === 0) return;

  const filesToUpload = Array.from(fileList);
  // 重置 input，允许重复选择同一文件
  input.value = '';

  uploading.value = true;
  uploadProgress.value = 0;
  try {
    for (let i = 0; i < filesToUpload.length; i++) {
      const file = filesToUpload[i]!;
      uploadFileName.value =
        filesToUpload.length > 1
          ? `${file.name} (${i + 1}/${filesToUpload.length})`
          : file.name;
      uploadProgress.value = 0;
      await uploadFileApi(
        selectedBucket.value,
        currentPrefix.value,
        file,
        (p) => {
          uploadProgress.value = p;
        },
      );
    }
    message.success(
      filesToUpload.length > 1
        ? `${filesToUpload.length} 个文件上传成功`
        : '上传成功',
    );
    resetPagination();
    await fetchEntries(true);
    await refreshBucketStats(true);
  } catch {
    // error handled by interceptor
  } finally {
    uploading.value = false;
    uploadProgress.value = 0;
    uploadFileName.value = '';
  }
}

// ==================== 文件操作 ====================
async function handleDownload(file: FileEntry) {
  try {
    const blob = await downloadObjectApi(selectedBucket.value, file.key);
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = file.name;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  } catch {
    // error handled
  }
}

async function handleDeleteFile(file: FileEntry) {
  await deleteObjectApi(selectedBucket.value, file.key);
  message.success('删除成功');
  resetPagination();
  await fetchEntries(true);
  await refreshBucketStats(true);
}

async function handleDeleteFolder(folder: FolderEntry) {
  await deleteFolderApi(selectedBucket.value, folder.prefix);
  message.success('文件夹已删除');
  resetPagination();
  await fetchEntries(true);
  await refreshBucketStats(true);
}

function handlePreview(file: FileEntry) {
  previewBucket.value = selectedBucket.value;
  previewFileKey.value = file.key;
  previewFileName.value = file.name;
  previewFileSize.value = file.size || 0;
  previewShow.value = true;
}

// ==================== 列表视图列定义 ====================
const columns = computed<DataTableColumns>(() => {
  return [
  {
    title: '名称',
    key: 'name',
    render(row: any) {
      const isFolder = row.type === 'folder';
      return h(
        'div',
        {
          class: 'flex items-center gap-2',
          style: 'min-width: 0; cursor: pointer',
          onClick: () => isFolder && handleFolderOpen(row as FolderEntry),
        },
        [
          h(FileTypeIcon, {
            category: isFolder
              ? 'folder'
              : getFileCategory(row.name, row.contentType),
            size: 20,
          }),
          h(
            'span',
            {
              style:
                'overflow: hidden; text-overflow: ellipsis; white-space: nowrap;',
            },
            row.name,
          ),
        ],
      );
    },
  },
  {
    title: '大小',
    key: 'size',
    width: 120,
    render(row: any) {
      if (row.type === 'folder') return '-';
      return formatFileSize((row as FileEntry).size);
    },
  },
  {
    title: '类型',
    key: 'type',
    width: 100,
    render(row: any) {
      if (row.type === 'folder') return '文件夹';
      const cat = getFileCategory(row.name, row.contentType);
      const typeMap: Record<string, string> = {
        image: '图片',
        video: '视频',
        audio: '音频',
        text: '文本',
        pdf: 'PDF',
        archive: '压缩包',
        code: '代码',
        file: '文件',
      };
      return typeMap[cat] || '文件';
    },
  },
  {
    title: '修改日期',
    key: 'createdAt',
    width: 170,
    render(row: any) {
      if (row.type === 'folder') return '-';
      return formatDate((row as FileEntry).createdAt);
    },
  },
  {
    title: '操作',
    key: 'actions',
    width: 240,
    render(row: any) {
      if (row.type === 'folder') {
        const folder = row as FolderEntry;
        return h(NSpace, { size: 'small' }, () => [
          h(
            NButton,
            {
              size: 'small',
              type: 'primary',
              quaternary: true,
              onClick: () => handleFolderOpen(folder),
            },
            { default: () => '打开' },
          ),
          h(
            NPopconfirm,
            {
              onPositiveClick: () => handleDeleteFolder(folder),
            },
            {
              trigger: () =>
                h(
                  NButton,
                  { size: 'small', type: 'error', quaternary: true },
                  { default: () => '删除' },
                ),
              default: () =>
                `确定删除文件夹 ${folder.name} 及其所有内容吗？`,
            },
          ),
        ]);
      }
      const file = row as FileEntry;
      return h(NSpace, { size: 'small' }, () => [
        h(
          NButton,
          {
            size: 'small',
            type: 'info',
            quaternary: true,
            onClick: () => handlePreview(file),
          },
          {
            icon: () => h(NIcon, null, { default: () => h(Eye) }),
            default: () => '查看',
          },
        ),
        h(
          NButton,
          {
            size: 'small',
            type: 'primary',
            quaternary: true,
            onClick: () => handleDownload(file),
          },
          {
            icon: () => h(NIcon, null, { default: () => h(Download) }),
            default: () => '下载',
          },
        ),
        h(
          NPopconfirm,
          { onPositiveClick: () => handleDeleteFile(file) },
          {
            trigger: () =>
              h(
                NButton,
                { size: 'small', type: 'error', quaternary: true },
                { default: () => '删除' },
              ),
            default: () => `确定删除文件 ${file.name} 吗？`,
          },
        ),
      ]);
    },
  },
];
});

function rowProps(row: any) {
  return {
    style: 'cursor: pointer',
    onDblclick: () => handleRowDblClick(row),
  };
}

function handleRowDblClick(row: any) {
  if (row.type === 'folder') {
    handleFolderOpen(row);
  } else {
    handlePreview(row as FileEntry);
  }
}

function entryCategory(entry: any): string {
  if (entry.type === 'folder') return 'folder';
  return getFileCategory(entry.name, entry.contentType);
}

</script>

<template>
  <div class="storage-page flex h-full flex-col p-2">
    <NCard :bordered="false" class="flex-1 rounded-lg" :content-style="{ padding: '0' }">
      <div class="storage-layout">
        <!-- 左侧存储桶列表 -->
        <div class="bucket-panel">
          <div class="bucket-header">
            <span class="bucket-title">存储桶</span>
            <div class="bucket-actions">
              <NTooltip>
                <template #trigger>
                  <NButton quaternary size="small" @click="fetchBuckets()">
                    <NIcon><RotateCw /></NIcon>
                  </NButton>
                </template>
                刷新
              </NTooltip>
              <NTooltip>
                <template #trigger>
                  <NButton
                    quaternary
                    size="small"
                    type="primary"
                    @click="handleCreateBucket"
                  >
                    <NIcon><Plus /></NIcon>
                  </NButton>
                </template>
                新建存储桶
              </NTooltip>
            </div>
          </div>
          <div class="bucket-list">
            <div
              v-for="b in buckets"
              :key="b.name"
              class="bucket-item"
              :class="{ active: b.name === selectedBucket }"
              @click="handleSelectBucket(b.name)"
            >
              <BucketIcon :size="18" />
              <div class="bucket-info">
                <div class="bucket-name">{{ b.name }}</div>
                <div class="bucket-meta">{{ b.objectCount }} 个对象</div>
              </div>
              <NPopconfirm @positive-click="handleDeleteBucket(b.name)">
                <template #trigger>
                  <NButton
                    quaternary
                    size="tiny"
                    type="error"
                    class="bucket-delete"
                    @click.stop
                  >
                    ×
                  </NButton>
                </template>
                确定删除存储桶 {{ b.name }} 及其所有文件吗？
              </NPopconfirm>
            </div>
            <NEmpty
              v-if="buckets.length === 0"
              description="暂无存储桶"
              :style="{ padding: '40px 0' }"
            />
          </div>
        </div>

        <!-- 右侧文件浏览区 -->
        <div class="file-panel">
          <template v-if="currentBucket">
            <!-- 头部统计 -->
            <div class="file-header">
              <div class="file-stats">
                <div class="stat-item">
                  <div class="stat-value">
                    {{ currentBucket.objectCount }}
                  </div>
                  <div class="stat-label">文件数</div>
                </div>
                <div class="stat-divider"></div>
                <div class="stat-item">
                  <div class="stat-value">
                    {{ formatFileSize(currentBucket.totalSize) }}
                  </div>
                  <div class="stat-label">占用空间</div>
                </div>
              </div>
              <div class="file-toolbar">
                <NTooltip>
                  <template #trigger>
                    <NButton
                      quaternary
                      size="small"
                      @click="resetPagination(); fetchEntries(true); refreshBucketStats(true)"
                    >
                      <NIcon><RotateCw /></NIcon>
                    </NButton>
                  </template>
                  刷新
                </NTooltip>
                <NButton
                  type="primary"
                  size="small"
                  :disabled="uploading"
                  @click="handleUploadClick"
                >
                  <template #icon><NIcon><Upload /></NIcon></template>
                  上传
                </NButton>
                <NButton
                  size="small"
                  :disabled="uploading"
                  @click="handleCreateFolder"
                >
                  <template #icon><NIcon><Plus /></NIcon></template>
                  新建目录
                </NButton>
                <NButton
                  :type="viewMode === 'list' ? 'primary' : 'default'"
                  quaternary
                  size="small"
                  @click="viewMode = 'list'"
                >
                  <NIcon><List /></NIcon>
                </NButton>
                <NButton
                  :type="viewMode === 'grid' ? 'primary' : 'default'"
                  quaternary
                  size="small"
                  @click="viewMode = 'grid'"
                >
                  <NIcon><Grid /></NIcon>
                </NButton>
              </div>
              <input
                ref="fileInputRef"
                type="file"
                multiple
                class="hidden-file-input"
                @change="handleFileChange"
              />
            </div>

            <!-- 面包屑导航 -->
            <div class="breadcrumb-bar">
              /
              <template
                v-for="(crumb, idx) in breadcrumbs"
                :key="idx"
              >
                <span
                  class="breadcrumb-item"
                  :class="{
                    clickable: idx < breadcrumbs.length - 1,
                  }"
                  @click="
                    idx < breadcrumbs.length - 1 && handleBreadcrumb(idx)
                  "
                >
                  {{ crumb.label || '根目录' }}
                </span>
                <NIcon
                  v-if="idx < breadcrumbs.length - 1"
                  class="breadcrumb-sep"
                >
                  <ChevronRight />
                </NIcon>
              </template>
            </div>

            <!-- 上传进度条 -->
            <div v-if="uploading" class="upload-progress-bar">
              <div class="upload-info">
                <span class="upload-name">{{ uploadFileName }}</span>
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

            <!-- 文件内容区 -->
            <div class="file-content">
              <NSpin :show="loading">
                <template v-if="!loading && allEntries.length === 0">
                  <NEmpty
                    description="此文件夹为空"
                    :style="{ padding: '60px 0' }"
                  />
                </template>
                <template v-else>
                  <!-- 列表视图 -->
                  <NDataTable
                    v-if="viewMode === 'list'"
                    :columns="columns"
                    :data="allEntries"
                    :row-key="(row: any) => row._key"
                    :row-props="rowProps"
                    :single-line="false"
                    size="small"
                    :bordered="false"
                  />

                  <!-- 宫格视图 -->
                  <div v-else class="grid-view">
                    <div
                      v-for="entry in allEntries"
                      :key="entry._key"
                      class="grid-item"
                      @dblclick="handleRowDblClick(entry)"
                    >
                      <FileTypeIcon
                        :category="entryCategory(entry)"
                        :size="48"
                      />
                      <div class="grid-name" :title="entry.name">
                        {{ entry.name }}
                      </div>
                      <div class="grid-meta">
                        <template v-if="entry.type === 'file'">
                          {{ formatFileSize((entry as FileEntry).size) }}
                        </template>
                        <template v-else>文件夹</template>
                      </div>
                      <div class="grid-actions">
                        <NPopconfirm
                          v-if="entry.type === 'folder'"
                          @positive-click="handleDeleteFolder(entry as FolderEntry)"
                        >
                          <template #trigger>
                            <NButton
                              quaternary
                              size="tiny"
                              type="error"
                              @click.stop
                            >
                              ×
                            </NButton>
                          </template>
                          删除该文件夹及其所有内容？
                        </NPopconfirm>
                        <NTooltip v-if="entry.type === 'file'">
                          <template #trigger>
                            <NButton
                              quaternary
                              size="tiny"
                              @click.stop="handlePreview(entry as FileEntry)"
                            >
                              <NIcon><Eye /></NIcon>
                            </NButton>
                          </template>
                          查看
                        </NTooltip>
                        <NTooltip v-if="entry.type === 'file'">
                          <template #trigger>
                            <NButton
                              quaternary
                              size="tiny"
                              @click.stop="handleDownload(entry as FileEntry)"
                            >
                              <NIcon><Download /></NIcon>
                            </NButton>
                          </template>
                          下载
                        </NTooltip>
                        <NPopconfirm
                          v-if="entry.type === 'file'"
                          @positive-click="handleDeleteFile(entry as FileEntry)"
                        >
                          <template #trigger>
                            <NButton
                              quaternary
                              size="tiny"
                              type="error"
                              @click.stop
                            >
                              ×
                            </NButton>
                          </template>
                          删除该文件？
                        </NPopconfirm>
                      </div>
                    </div>
                  </div>

                  <!-- 服务端分页栏（列表/宫格共用） -->
                  <div class="pagination-bar">
                    <div class="pagination-info">
                      本页 {{ allEntries.length }} 项
                      <template v-if="indexTruncated">
                        （仅索引前 50000 个对象）
                      </template>
                    </div>
                    <NSpace align="center" :size="8">
                      <NButton
                        size="small"
                        :disabled="currentPage <= 1 || loading"
                        @click="gotoPage(currentPage - 1)"
                      >
                        上一页
                      </NButton>
                      <span class="page-indicator">第 {{ currentPage }} 页</span>
                      <NButton
                        size="small"
                        :disabled="!hasNextPage || loading"
                        @click="gotoPage(currentPage + 1)"
                      >
                        下一页
                      </NButton>
                      <NSelect
                        v-model:value="pageSize"
                        size="small"
                        :options="pageSizeSelectOptions"
                        :style="{ width: '110px' }"
                        @update:value="handlePageSizeChange"
                      />
                    </NSpace>
                  </div>
                </template>
              </NSpin>
            </div>
          </template>

          <NEmpty
            v-else
            description="请选择或创建一个存储桶"
            :style="{ padding: '80px 0' }"
          />
        </div>
      </div>
    </NCard>

    <!-- 新建存储桶弹窗 -->
    <WwModal
      v-model:show="showCreateBucket"
      title="新建存储桶"
      :width="420"
      :mask-closable="false"
      confirm-text="创建"
      cancel-text="取消"
      :confirm-loading="creatingBucket"
      :maximizable="false"
      :resizable="false"
      @confirm="handleSubmitCreateBucket"
    >
      <div>
        <NInput
          v-model:value="newBucketName"
          placeholder="请输入存储桶名称（小写字母、数字、中划线）"
          @keyup.enter="handleSubmitCreateBucket"
        />
        <div class="mt-2 text-xs text-gray-400">
          名称只能包含小写字母、数字和中划线，长度 3-63 位
        </div>
      </div>
    </WwModal>

    <!-- 新建目录弹窗 -->
    <WwModal
      v-model:show="showCreateFolder"
      title="新建目录"
      :width="420"
      :mask-closable="false"
      confirm-text="创建"
      cancel-text="取消"
      :confirm-loading="creatingFolder"
      :maximizable="false"
      :resizable="false"
      @confirm="handleSubmitCreateFolder"
    >
      <div>
        <NInput
          v-model:value="newFolderName"
          placeholder="请输入目录名称"
          @keyup.enter="handleSubmitCreateFolder"
        />
        <div class="mt-2 text-xs text-gray-400">
          目录将在当前路径下创建，名称不能包含 /
        </div>
      </div>
    </WwModal>

    <!-- 文件预览弹窗 -->
    <FilePreview
      v-model:show="previewShow"
      :bucket="previewBucket"
      :file-key="previewFileKey"
      :file-name="previewFileName"
      :file-size="previewFileSize"
    />
  </div>
</template>

<style scoped>
.storage-layout {
  display: flex;
  /* 高度由 layoutHeight 内联样式实时控制，这里仅为 JS 就绪前的兜底 */
  height: 600px;
}

.bucket-panel {
  width: 240px;
  min-width: 240px;
  border-right: 1px solid var(--n-border-color, #e5e7eb);
  display: flex;
  flex-direction: column;
}

.bucket-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 12px 16px;
  border-bottom: 1px solid var(--n-border-color, #e5e7eb);
}

.bucket-title {
  font-weight: 600;
  font-size: 14px;
}

.bucket-actions {
  display: flex;
  gap: 4px;
}

.bucket-list {
  flex: 1;
  overflow-y: auto;
  padding: 4px;
}

.bucket-item {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 12px;
  border-radius: 6px;
  cursor: pointer;
  transition: background-color 0.15s;
}

.bucket-item:hover {
  background-color: var(--n-color-hover, rgba(0, 0, 0, 0.04));
}

.bucket-item.active {

  background-color: rgba(24, 160, 240, 0.2);
}

.bucket-info {
  flex: 1;
  min-width: 0;
}

.bucket-name {
  font-size: 13px;
  font-weight: 500;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.bucket-meta {
  font-size: 11px;
  color: var(--n-text-color-3, #999);
}

.bucket-delete {
  opacity: 0;
  transition: opacity 0.15s;
}

.bucket-item:hover .bucket-delete {
  opacity: 1;
}

.file-panel {
  flex: 1;
  display: flex;
  flex-direction: column;
  min-width: 0;
}

.file-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 16px 20px;
  border-bottom: 1px solid var(--n-border-color, #e5e7eb);
}

.file-stats {
  display: flex;
  align-items: center;
  gap: 24px;
}

.stat-item {
  display: flex;
  flex-direction: column;
}

.stat-value {
  font-size: 20px;
  font-weight: 600;
  color: var(--n-text-color, #333);
}

.stat-label {
  font-size: 12px;
  color: var(--n-text-color-3, #999);
  margin-top: 2px;
}

.stat-divider {
  width: 1px;
  height: 32px;
  background-color: var(--n-border-color, #e5e7eb);
}

.file-toolbar {
  display: flex;
  align-items: center;
  gap: 4px;
}

.hidden-file-input {
  position: absolute;
  width: 0;
  height: 0;
  opacity: 0;
  overflow: hidden;
  pointer-events: none;
}

.upload-progress-bar {
  padding: 8px 16px;
  border-bottom: 1px solid var(--n-border-color, #e5e7eb);
  background-color: var(--n-color-modal, #fafafa);
}

.upload-info {
  display: flex;
  justify-content: space-between;
  margin-bottom: 4px;
  font-size: 12px;
}

.upload-name {
  color: var(--n-text-color-2, #666);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  max-width: 80%;
}

.upload-percent {
  color: var(--n-primary-color, #1890ff);
  font-weight: 600;
}

.breadcrumb-bar {
  display: flex;
  align-items: center;
  padding: 8px 8px;
  border-bottom: 1px solid var(--n-border-color, #e5e7eb);
  background-color: var(--n-color-modal, #fafafa);
  flex-wrap: wrap;
}

.breadcrumb-item {
  font-size: 13px;
  color: var(--n-text-color-2, #666);
  padding: 2px 6px;
  border-radius: 4px;
}

.breadcrumb-item.clickable {
  cursor: pointer;
  color: var(--n-primary-color, #1890ff);
}

.breadcrumb-item.clickable:hover {
  background-color: var(--n-color-hover, rgba(0, 0, 0, 0.04));
}

.breadcrumb-sep {
  font-size: 14px;
  color: var(--n-text-color-3, #ccc);
}

.file-content {
  flex: 1;
  min-height: 0;
  overflow: auto;
  padding: 0px 0px;
  display: flex;
  flex-direction: column;
}

.pagination-bar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 10px 12px;
  border-top: 1px solid var(--n-border-color, #e5e7eb);
}

.pagination-info {
  font-size: 12px;
  color: var(--n-text-color-3, #999);
}

.page-indicator {
  font-size: 13px;
  min-width: 56px;
  text-align: center;
  display: inline-block;
}

.grid-view {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(120px, 1fr));
  gap: 12px;
  padding: 12px 4px;
}

.grid-item {
  display: flex;
  flex-direction: column;
  align-items: center;
  padding: 16px 8px 8px;
  border-radius: 8px;
  cursor: pointer;
  transition: background-color 0.15s;
  position: relative;
  border: 1px solid transparent;
}

.grid-item:hover {
  background-color: var(--n-color-hover, rgba(0, 0, 0, 0.04));
  border-color: var(--n-border-color, #e5e7eb);
}

.grid-name {
  margin-top: 8px;
  font-size: 12px;
  text-align: center;
  width: 100%;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.grid-meta {
  margin-top: 4px;
  font-size: 11px;
  color: var(--n-text-color-3, #999);
}

.grid-actions {
  position: absolute;
  top: 4px;
  right: 4px;
  display: none;
}

.grid-item:hover .grid-actions {
  display: flex;
}
</style>
