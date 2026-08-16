<script setup lang="ts">
import { computed, ref, watch } from 'vue';

import { useAppConfig } from '@vben/hooks';
import { useAccessStore } from '@vben/stores';

import { FileViewer } from '@file-viewer/vue3-full';
import {
  NAlert,
  NButton,
  NProgress,
  NSpace,
  NSpin,
} from 'naive-ui';

import {
  downloadObjectApi,
  formatFileSize,
  getFileCategory,
  getObjectStreamUrl,
} from '#/api/core/storage';
import { WwModal } from '#/components/WwModal';

const props = withDefaults(defineProps<Props>(), {
  bucket: '',
  fileKey: '',
  fileName: '',
  fileSize: 0,
  width: '90vw',
});

const emit = defineEmits<{
  'update:show': [value: boolean];
}>();

const { apiURL } = useAppConfig(import.meta.env, import.meta.env.PROD);

interface Props {
  show: boolean;
  bucket?: string;
  fileKey?: string;
  fileName?: string;
  fileSize?: number;
  width?: string;
}

const LARGE_FILE_THRESHOLD = 100 * 1024 * 1024; // 100MB

/** 浏览器原生可播放的媒体格式（走 Range 流式播放；其余退化为全量下载） */
const NATIVE_VIDEO_EXTS = ['m4v', 'mov', 'mp4', 'webm'];
const NATIVE_AUDIO_EXTS = ['aac', 'flac', 'm4a', 'mp3', 'ogg', 'wav'];

const fileExt = computed(
  () => (props.fileName.split('.').pop() || '').toLowerCase(),
);
/** 可流式播放时返回 video/audio，否则返回空串（走全量下载 + FileViewer） */
const mediaType = computed<'' | 'audio' | 'video'>(() => {
  if (NATIVE_VIDEO_EXTS.includes(fileExt.value)) return 'video';
  if (NATIVE_AUDIO_EXTS.includes(fileExt.value)) return 'audio';
  return '';
});
/** 流式播放直链（仅 mediaType 非空时赋值） */
const streamUrl = ref('');

/** 文本/代码类文件：走 fetch 流式加载，边下边渲染 */
const MAX_TEXT_PREVIEW_BYTES = 5 * 1024 * 1024; // 5MB（避免超大文本撑爆 DOM）
const fileCategory = computed(() => getFileCategory(props.fileName));
const isTextLike = computed(
  () => fileCategory.value === 'code' || fileCategory.value === 'text',
);
const streamingText = ref('');
const textTruncated = ref(false);
let textAbortController: AbortController | null = null;

const loading = ref(false);
const errorMsg = ref('');
const previewFile = ref<File | null>(null);
const downloadProgress = ref(0);
const downloadedBytes = ref(0);
const totalBytes = ref(0);
const showLargeFileConfirm = ref(false);
const largeFileConfirmed = ref(false);
/** 防止快速切换时旧请求覆盖新结果的令牌 */
let loadToken = 0;

const modalShow = computed({
  get: () => props.show,
  set: (v) => emit('update:show', v),
});

const isLargeFile = computed(
  () => props.fileSize > 0 && props.fileSize >= LARGE_FILE_THRESHOLD,
);

const progressPercentage = computed(() => {
  if (totalBytes.value > 0) {
    return Math.round((downloadedBytes.value / totalBytes.value) * 100);
  }
  return downloadProgress.value;
});

const progressText = computed(() => {
  if (totalBytes.value > 0) {
    return `${formatFileSize(downloadedBytes.value)} / ${formatFileSize(totalBytes.value)}`;
  }
  return `${downloadProgress.value}%`;
});

const viewerOptions = {
  theme: 'light' as const,
  styleIsolation: 'shadow' as const,
  ui: { density: 'compact' as const },
  toolbar: {
    position: 'bottom-right' as const,
    download: false,
    print: true,
    zoom: true,
  },
};

function resetState() {
  ++loadToken; // 令所有进行中的请求失效
  textAbortController?.abort();
  textAbortController = null;
  previewFile.value = null;
  errorMsg.value = '';
  loading.value = false;
  downloadProgress.value = 0;
  downloadedBytes.value = 0;
  totalBytes.value = 0;
  streamUrl.value = '';
  streamingText.value = '';
  textTruncated.value = false;
}

async function loadFile() {
  if (!props.bucket || !props.fileKey || !props.fileName) return;

  // 视频/音频：直接构造 Range 流式直链，浏览器按需分段拉取，边下边播
  if (mediaType.value) {
    showLargeFileConfirm.value = false;
    loading.value = false;
    streamUrl.value = getObjectStreamUrl(props.bucket, props.fileKey);
    return;
  }

  // 文本/代码：fetch 流式读取，边下边渲染（展示上限内无需全量下载）
  if (isTextLike.value) {
    void loadTextStream();
    return;
  }

  // 大文件需要用户确认
  if (isLargeFile.value && !largeFileConfirmed.value) {
    showLargeFileConfirm.value = true;
    return;
  }

  showLargeFileConfirm.value = false;
  loading.value = true;
  errorMsg.value = '';
  previewFile.value = null;
  downloadProgress.value = 0;
  downloadedBytes.value = 0;
  totalBytes.value = props.fileSize || 0;

  const token = ++loadToken;
  try {
    const blob = await downloadObjectApi(
      props.bucket,
      props.fileKey,
      (loaded, total) => {
        if (token !== loadToken) return;
        downloadedBytes.value = loaded;
        totalBytes.value = total || props.fileSize || 0;
        if (total > 0) {
          downloadProgress.value = Math.round((loaded / total) * 100);
        }
      },
    );
    if (token !== loadToken) return; // 已被新的请求取代，丢弃旧结果
    previewFile.value = new File([blob], props.fileName, {
      type: blob.type || 'application/octet-stream',
    });
  } catch (e: any) {
    if (token !== loadToken) return;
    errorMsg.value = e?.message || '文件加载失败';
  } finally {
    if (token === loadToken) loading.value = false;
  }
}

/** 流式加载文本文件：逐块解码并追加渲染，超出展示上限后中断下载 */
async function loadTextStream() {
  const token = ++loadToken;
  textAbortController?.abort();
  const controller = new AbortController();
  textAbortController = controller;

  showLargeFileConfirm.value = false;
  loading.value = true;
  errorMsg.value = '';
  streamingText.value = '';
  textTruncated.value = false;
  downloadProgress.value = 0;
  downloadedBytes.value = 0;
  totalBytes.value = props.fileSize || 0;

  try {
    const accessStore = useAccessStore();
    const response = await fetch(
      `${apiURL}/storage/buckets/${encodeURIComponent(props.bucket)}/object`,
      {
        headers: {
          Authorization: `Bearer ${accessStore.accessToken || ''}`,
          'X-Object-Key': encodeURIComponent(props.fileKey),
        },
        signal: controller.signal,
      },
    );
    if (!response.ok) {
      throw new Error(`下载失败（HTTP ${response.status}）`);
    }
    if (token !== loadToken) return;
    totalBytes.value =
      Number(response.headers.get('Content-Length')) || props.fileSize || 0;
    const reader = response.body?.getReader();
    if (!reader) {
      throw new Error('当前浏览器不支持流式读取');
    }
    // stream: true 保证多字节字符跨分块时正确解码
    const decoder = new TextDecoder('utf-8');
    while (true) {
      const { done, value } = await reader.read();
      if (done || token !== loadToken) break;
      downloadedBytes.value += value.byteLength;
      if (totalBytes.value > 0) {
        downloadProgress.value = Math.round(
          (downloadedBytes.value / totalBytes.value) * 100,
        );
      }
      if (downloadedBytes.value <= MAX_TEXT_PREVIEW_BYTES) {
        streamingText.value += decoder.decode(value, { stream: true });
      } else if (!textTruncated.value) {
        textTruncated.value = true;
        await reader.cancel();
        break;
      }
    }
    if (token === loadToken) {
      streamingText.value += decoder.decode();
    }
  } catch (e: any) {
    if (token !== loadToken || e?.name === 'AbortError') return;
    errorMsg.value = e?.message || '文件加载失败';
  } finally {
    if (token === loadToken) loading.value = false;
  }
}

function confirmLargeFile() {
  largeFileConfirmed.value = true;
  showLargeFileConfirm.value = false;
  loadFile();
}

function cancelLargeFile() {
  showLargeFileConfirm.value = false;
  largeFileConfirmed.value = false;
  modalShow.value = false;
}

function handleClose() {
  modalShow.value = false;
}

function handleRetry() {
  loadFile();
}

watch(
  () => [props.show, props.bucket, props.fileKey] as const,
  ([visible]) => {
    if (visible) {
      resetState();
      largeFileConfirmed.value = false;
      showLargeFileConfirm.value = false;
      loadFile();
    } else {
      resetState();
      showLargeFileConfirm.value = false;
      largeFileConfirmed.value = false;
    }
  },
  { immediate: true },
);
</script>

<template>
  <WwModal
    v-model:show="modalShow"
    :title="fileName || '文件预览'"
    :width="width"
    height="85vh"
    :mask-closable="false"
    :footer="false"
    default-maximized
    content-class="file-preview-content"
    @close="handleClose"
  >
    <div class="file-preview-body">
      <!-- 视频/音频流式播放（Range 分段拉取，边下边播） -->
      <div v-if="streamUrl" class="media-player-wrap">
        <video
          v-if="mediaType === 'video'"
          :src="streamUrl"
          class="media-player"
          controls
          preload="metadata"
        ></video>
        <audio v-else :src="streamUrl" class="w-full" controls></audio>
      </div>

      <!-- 文本/代码流式加载（边下边渲染；出错时落到下方错误分支） -->
      <div v-else-if="isTextLike && !errorMsg" class="text-viewer-wrap">
        <div v-if="loading || textTruncated" class="text-progress">
          <NProgress
            v-if="loading"
            type="line"
            :percentage="progressPercentage"
            :height="6"
            :border-radius="3"
            :show-indicator="false"
            status="success"
            style="flex: 1"
          />
          <span v-if="loading">{{ progressText }}</span>
        </div>
        <NSpin v-if="loading && !streamingText" class="text-loading" />
        <pre v-else class="text-viewer">{{
          streamingText || '（空文件）'
        }}</pre>
        <NAlert v-if="textTruncated" type="warning" :show-icon="true">
          文件较大，仅流式展示前 {{ formatFileSize(MAX_TEXT_PREVIEW_BYTES)
          }}，完整内容请下载查看
        </NAlert>
      </div>

      <!-- 大文件确认提示 -->
      <NAlert
        v-else-if="showLargeFileConfirm"
        type="warning"
        title="大文件预览提示"
        :show-icon="true"
        style="margin: 24px"
      >
        <div style="margin-bottom: 12px">
          文件大小为 <strong>{{ formatFileSize(fileSize) }}</strong>，完整加载可能需要较长时间并占用较多内存。是否继续预览？
        </div>
        <NSpace justify="end">
          <NButton size="small" @click="cancelLargeFile">取消</NButton>
          <NButton size="small" type="primary" @click="confirmLargeFile">
            继续预览
          </NButton>
        </NSpace>
      </NAlert>

      <!-- 加载中 -->
      <div v-else-if="loading" class="preview-loading">
        <NSpin description="文件加载中..." size="large">
          <div class="loading-placeholder"></div>
        </NSpin>
        <div v-if="downloadProgress > 0 || totalBytes > 0" class="progress-wrapper">
          <NProgress
            type="line"
            :percentage="progressPercentage"
            :height="8"
            :border-radius="4"
            :show-indicator="false"
            status="success"
          />
          <div class="progress-text">{{ progressText }}</div>
        </div>
      </div>

      <!-- 错误 -->
      <NAlert
        v-else-if="errorMsg"
        type="error"
        title="预览失败"
        :show-icon="true"
        style="margin: 24px"
      >
        <NSpace align="center" justify="space-between">
          <span>{{ errorMsg }}</span>
          <NButton size="small" type="primary" @click="handleRetry">
            重试
          </NButton>
        </NSpace>
      </NAlert>

      <!-- 预览内容 -->
      <div v-else-if="previewFile" class="file-viewer-container">
        <FileViewer :file="previewFile" :options="viewerOptions" />
      </div>

      <!-- 空状态 -->
      <NAlert
        v-else
        type="info"
        :show-icon="false"
        style="margin: 24px"
      >
        暂无可预览的文件
      </NAlert>
    </div>
  </WwModal>
</template>

<!-- 弹窗 Teleport 到 body，内容区样式需非 scoped 才能命中 -->
<style>
.file-preview-content {
  padding: 8px;
}
</style>

<style scoped>
.file-preview-body {
  flex: 1;
  min-height: 400px;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

.preview-loading {
  flex: 1;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 24px;
}

.loading-placeholder {
  height: 200px;
}

.progress-wrapper {
  width: 60%;
  max-width: 480px;
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.progress-text {
  text-align: center;
  font-size: 12px;
  color: var(--n-text-color-3, #999);
}

.file-viewer-container {
  flex: 1;
  min-height: 0;
  overflow: hidden;
  border: 1px solid var(--n-border-color, #efeff5);
  border-radius: 4px;
}

.file-viewer-container :deep(.flyfish-file-viewer),
.file-viewer-container :deep([class*='file-viewer']),
.file-viewer-container :deep([class*='flyfish']) {
  height: 100%;
}

.media-player-wrap {
  flex: 1;
  min-height: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  background: #000;
  border-radius: 4px;
  overflow: hidden;
}

.media-player {
  width: 100%;
  height: 100%;
  object-fit: contain;
}

.text-viewer-wrap {
  flex: 1;
  min-height: 0;
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.text-progress {
  display: flex;
  align-items: center;
  gap: 12px;
  font-size: 12px;
  color: var(--n-text-color-3, #999);
}

.text-loading {
  flex: 1;
}

.text-viewer {
  flex: 1;
  min-height: 0;
  margin: 0;
  padding: 12px;
  overflow: auto;
  background-color: var(--n-color-modal, #fafafa);
  border: 1px solid var(--n-border-color, #efeff5);
  border-radius: 4px;
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 12px;
  line-height: 1.6;
  white-space: pre-wrap;
  word-break: break-all;
}
</style>
