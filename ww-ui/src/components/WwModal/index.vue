<script setup lang="ts">
import type { CSSProperties } from 'vue';

import type {
  ResizeHandleDirection,
  SizeValue,
  WwModalProps,
} from './types';

import { computed, nextTick, onBeforeUnmount, reactive, ref, watch } from 'vue';

import { useEventListener } from '@vueuse/core';
import { NButton, NSpin } from 'naive-ui';

import { modalStack } from './modal-stack';

/**
 * WwModal 通用弹出层组件
 *
 * 基于 naive-ui 封装，支持：
 * - 最大化 / 最小化 / 还原
 * - 头部按住拖拽移动
 * - 拖拽边框与四角缩放
 * - 遮罩开关、点击遮罩关闭、自定义遮罩样式
 * - 自定义基础 zIndex，多弹窗共存时层级自动切换（点击谁谁置顶）
 * - 外链 iframe 内容
 * - 自定义弹出/关闭动画（transitionName）
 * - 保留 naive 风格的基础能力：标题、描述、loading、底部确认/取消按钮、ESC 关闭等
 */
defineOptions({ name: 'WwModal' });

const props = withDefaults(
  defineProps<
    {
      /** 是否显示（v-model:show） */
      show?: boolean;
    } & WwModalProps
  >(),
  {
    appendTo: 'body',
    cancelText: '取消',
    centered: true,
    closable: true,
    closeOnEsc: true,
    confirmText: '确认',
    defaultMaximized: false,
    destroyOnClose: false,
    draggable: true,
    duration: undefined,
    footer: true,
    height: undefined,
    iframeSrc: '',
    loading: false,
    mask: false,
    maskClosable: true,
    maskStyle: undefined,
    maximizable: true,
    minHeight: 120,
    minimizable: false,
    minWidth: 280,
    offsetX: 0,
    offsetY: 0,
    onBeforeClose: undefined,
    resizable: true,
    show: false,
    showCancelButton: true,
    showConfirmButton: true,
    title: '',
    transitionName: 'ww-modal-zoom',
    width: 520,
    zIndex: undefined,
  },
);
const emit = defineEmits<{
  cancel: [];
  close: [];
  closed: [];
  confirm: [];
  iframeLoad: [event: Event];
  maximize: [];
  minimize: [];
  open: [];
  opened: [];
  restore: [];
  'update:show': [value: boolean];
}>();
const HEADER_HEIGHT = 45;
const MINIMIZED_WIDTH = 240;
const MINIMIZED_MARGIN = 12;
const RESIZE_HANDLES: ResizeHandleDirection[] = [
  'n',
  'ne',
  'e',
  'se',
  's',
  'sw',
  'w',
  'nw',
];

const id = Symbol('ww-modal');
const modalRef = ref<HTMLElement>();
const iframeRef = ref<HTMLIFrameElement>();

/** 控制弹窗主体显隐（配合 Transition 保留离场动画） */
const visible = ref(false);
/** 内容是否渲染（destroyOnClose 时在离场动画结束后销毁） */
const rendered = ref(false);
const iframeLoaded = ref(false);
const dragging = ref(false);
const resizing = ref<'' | ResizeHandleDirection>('');

/** 当前弹窗层级（由弹窗栈分配） */
const currentZIndex = ref(2000);
const isMaximized = ref(false);
const isMinimized = ref(false);
/** 最小化前是否处于最大化状态（还原时恢复） */
let minimizedFromMaximized = false;

/** 弹窗位置与尺寸 */
const rect = reactive({
  height: undefined as number | undefined,
  left: 0,
  top: 0,
  width: 520,
});
/** 最大化/最小化前的位置与尺寸，用于还原 */
const savedRect = {
  height: undefined as number | undefined,
  left: 0,
  top: 0,
  width: 520,
};

/** 尺寸归一化：支持 number / 'px' / 'vw' / 'vh' / '%' */
function normalizeSize(value: SizeValue | undefined, refSize: number): number {
  if (value == null) return 0;
  if (typeof value === 'number') return value;
  const matched = /^(-?\d+(?:\.\d+)?)(%|px|vh|vw)?$/.exec(value.trim());
  if (!matched) return 0;
  const num = Number(matched[1]);
  switch (matched[2]) {
    case '%':
    case 'vh':
    case 'vw': {
      return (num / 100) * refSize;
    }
    default: {
      return num;
    }
  }
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(Math.max(value, min), max);
}

/** 重置为初始位置与尺寸（居中） */
function resetRect() {
  const width = Math.max(
    props.minWidth,
    Math.min(normalizeSize(props.width, window.innerWidth) || 520, window.innerWidth - 32),
  );
  const height = props.height
    ? normalizeSize(props.height, window.innerHeight)
    : undefined;
  rect.width = width;
  rect.height = height;
  rect.left = clamp(
    Math.max(16, (window.innerWidth - width) / 2) + props.offsetX,
    16 - width / 2,
    window.innerWidth - 48,
  );
  rect.top = Math.max(
    0,
    (props.centered && height
      ? Math.max(32, (window.innerHeight - height) / 2)
      : Math.max(64, window.innerHeight * 0.15)) + props.offsetY,
  );
}

/** 高度自适应时，渲染后测量实际高度并垂直居中 */
async function centerVertical() {
  if (!props.centered || rect.height != null) return;
  await nextTick();
  const el = modalRef.value;
  if (!el || !visible.value) return;
  rect.top = Math.max(
    0,
    (window.innerHeight - Math.min(el.offsetHeight, window.innerHeight - 64)) /
      2 +
      props.offsetY,
  );
}

function openModal() {
  rendered.value = true;
  visible.value = true;
  iframeLoaded.value = false;
  isMinimized.value = false;
  modalStack.setMinimized(id, false);
  isMaximized.value = props.defaultMaximized;
  resetRect();
  currentZIndex.value = modalStack.register(id, props.zIndex);
  emit('open');
  void centerVertical();
}

function closeModal() {
  if (!visible.value) {
    modalStack.unregister(id);
    return;
  }
  visible.value = false;
  modalStack.unregister(id);
  emit('close');
}

watch(
  () => props.show,
  (show) => {
    if (show) {
      openModal();
    } else {
      closeModal();
    }
  },
  { immediate: true },
);

onBeforeUnmount(() => {
  modalStack.unregister(id);
});

/** 关闭请求（支持 onBeforeClose 拦截） */
async function requestClose() {
  if (props.onBeforeClose) {
    const result = await props.onBeforeClose();
    if (result === false) return;
  }
  emit('update:show', false);
}

/** 将弹窗提升到最顶层（多弹窗层级自动切换） */
function bringToTop() {
  if (!props.show || isMinimized.value) return;
  currentZIndex.value = modalStack.toTop(id);
}

function saveRect() {
  savedRect.left = rect.left;
  savedRect.top = rect.top;
  savedRect.width = rect.width;
  savedRect.height = rect.height ?? modalRef.value?.offsetHeight;
}

function applySavedRect() {
  rect.left = savedRect.left;
  rect.top = savedRect.top;
  rect.width = savedRect.width;
  rect.height = savedRect.height;
}

/** 最大化 / 还原切换 */
function toggleMaximize() {
  if (!props.maximizable || isMinimized.value) return;
  if (isMaximized.value) {
    isMaximized.value = false;
    applySavedRect();
    emit('restore');
  } else {
    saveRect();
    isMaximized.value = true;
    emit('maximize');
  }
  bringToTop();
}

/** 最小化（收纳到视口底部） */
function minimize() {
  if (!props.minimizable || isMinimized.value) return;
  minimizedFromMaximized = isMaximized.value;
  if (!isMaximized.value) {
    saveRect();
  }
  isMinimized.value = true;
  modalStack.setMinimized(id, true);
  const index = modalStack.minimizedIndex(id);
  rect.width = MINIMIZED_WIDTH;
  rect.height = HEADER_HEIGHT;
  rect.left = Math.max(
    0,
    window.innerWidth -
      MINIMIZED_MARGIN -
      MINIMIZED_WIDTH -
      index * (MINIMIZED_WIDTH + MINIMIZED_MARGIN),
  );
  rect.top = window.innerHeight - HEADER_HEIGHT - MINIMIZED_MARGIN;
  emit('minimize');
}

/** 从最小化还原 */
function restore() {
  if (!isMinimized.value) return;
  isMinimized.value = false;
  modalStack.setMinimized(id, false);
  isMaximized.value = minimizedFromMaximized;
  if (!isMaximized.value) {
    applySavedRect();
  }
  bringToTop();
  emit('restore');
}

/** 点击最小化后的弹窗头部：还原 */
function onMinimizedHeaderClick() {
  if (isMinimized.value) {
    restore();
  }
}

function onCancel() {
  emit('cancel');
  void requestClose();
}

function onConfirm() {
  emit('confirm');
}

function onIframeLoad(event: Event) {
  iframeLoaded.value = true;
  emit('iframeLoad', event);
}

function onAfterLeave() {
  if (props.destroyOnClose) {
    rendered.value = false;
  }
  emit('closed');
}

/** 头部按下：开始拖拽移动 */
function onHeaderMousedown(event: MouseEvent) {
  if (isMinimized.value) return;
  if (!props.draggable || isMaximized.value) return;
  // 点到操作按钮不触发拖拽
  if ((event.target as HTMLElement).closest('.ww-modal__actions')) return;
  event.preventDefault();
  bringToTop();
  dragging.value = true;
  const startX = event.clientX;
  const startY = event.clientY;
  const startLeft = rect.left;
  const startTop = rect.top;

  const onMove = (moveEvent: MouseEvent) => {
    const height = modalRef.value?.offsetHeight ?? HEADER_HEIGHT;
    rect.left = clamp(
      startLeft + moveEvent.clientX - startX,
      48 - rect.width,
      window.innerWidth - 48,
    );
    rect.top = clamp(
      startTop + moveEvent.clientY - startY,
      0,
      window.innerHeight - Math.min(HEADER_HEIGHT, height),
    );
  };
  const onUp = () => {
    dragging.value = false;
    window.removeEventListener('mousemove', onMove);
    window.removeEventListener('mouseup', onUp);
  };
  window.addEventListener('mousemove', onMove);
  window.addEventListener('mouseup', onUp);
}

/** 拖柄按下：开始缩放 */
function onResizeStart(handle: ResizeHandleDirection, event: MouseEvent) {
  if (!props.resizable || isMaximized.value || isMinimized.value) return;
  event.preventDefault();
  event.stopPropagation();
  bringToTop();
  resizing.value = handle;
  // 高度自适应时，缩放开始即固定高度
  if (rect.height == null) {
    rect.height = modalRef.value?.offsetHeight ?? props.minHeight;
  }
  const startX = event.clientX;
  const startY = event.clientY;
  const start = { ...rect, height: rect.height as number };

  const onMove = (moveEvent: MouseEvent) => {
    const dx = moveEvent.clientX - startX;
    const dy = moveEvent.clientY - startY;
    let { height, left, top, width } = start;
    if (handle.includes('e')) width = start.width + dx;
    if (handle.includes('s')) height = start.height + dy;
    if (handle.includes('w')) {
      width = start.width - dx;
      left = start.left + dx;
    }
    if (handle.includes('n')) {
      height = start.height - dy;
      top = start.top + dy;
    }
    if (width < props.minWidth) {
      if (handle.includes('w')) left -= props.minWidth - width;
      width = props.minWidth;
    }
    if (height < props.minHeight) {
      if (handle.includes('n')) top -= props.minHeight - height;
      height = props.minHeight;
    }
    rect.left = left;
    rect.top = top;
    rect.width = width;
    rect.height = height;
  };
  const onUp = () => {
    resizing.value = '';
    window.removeEventListener('mousemove', onMove);
    window.removeEventListener('mouseup', onUp);
  };
  window.addEventListener('mousemove', onMove);
  window.addEventListener('mouseup', onUp);
}

/** ESC 仅关闭最顶层弹窗 */
useEventListener(document, 'keydown', (event: KeyboardEvent) => {
  if (event.key !== 'Escape' || !props.show || !props.closeOnEsc) return;
  if (!modalStack.isTop(id)) return;
  void requestClose();
});

/** iframe 内部点击不会冒泡到外层，通过 window blur 检测焦点进入 iframe 后置顶 */
useEventListener(window, 'blur', () => {
  requestAnimationFrame(() => {
    if (
      props.show &&
      iframeRef.value &&
      document.activeElement === iframeRef.value
    ) {
      bringToTop();
    }
  });
});

const modalStyle = computed<CSSProperties>(() => {
  const style: CSSProperties = {
    zIndex: currentZIndex.value,
  };
  if (props.duration != null) {
    style['--ww-modal-duration'] = `${props.duration}ms`;
  }
  if (isMaximized.value) {
    style.left = '0px';
    style.top = '0px';
    style.width = '100%';
    style.height = '100%';
    style.borderRadius = '0';
    return style;
  }
  style.left = `${rect.left}px`;
  style.top = `${rect.top}px`;
  style.width = `${rect.width}px`;
  if (rect.height != null) {
    style.height = `${rect.height}px`;
  } else {
    style.maxHeight = 'calc(100vh - 64px)';
  }
  return style;
});

const maskStyle = computed<CSSProperties>(() => ({
  ...props.maskStyle,
  zIndex: currentZIndex.value - 5,
  ...(props.duration != null
    ? { '--ww-modal-duration': `${props.duration}ms` }
    : {}),
}));

defineExpose({
  /** 置顶 */
  bringToTop,
  /** 关闭 */
  close: requestClose,
  /** 最大化/还原切换 */
  maximize: toggleMaximize,
  /** 最小化 */
  minimize,
  /** 从最小化还原 */
  restore,
});
</script>

<template>
  <Teleport :to="appendTo">
    <!-- 遮罩 -->
    <Transition name="ww-modal-mask" appear>
      <div
        v-show="show && mask && !isMinimized"
        class="ww-modal__mask"
        :style="maskStyle"
        @click="maskClosable && requestClose()"
      ></div>
    </Transition>

    <!-- 弹窗主体 -->
    <Transition
      :name="transitionName"
      appear
      @after-enter="emit('opened')"
      @after-leave="onAfterLeave"
    >
      <div
        v-show="visible"
        ref="modalRef"
        role="dialog"
        aria-modal="true"
        class="ww-modal"
        :class="{
          'is-dragging': dragging,
          'is-maximized': isMaximized,
          'is-minimized': isMinimized,
          'is-resizing': resizing,
        }"
        :style="modalStyle"
        @mousedown="bringToTop"
      >
        <!-- 缩放拖柄：四边 + 四角 -->
        <template v-if="resizable && !isMaximized && !isMinimized">
          <div
            v-for="handle in RESIZE_HANDLES"
            :key="handle"
            class="ww-modal__resize-handle"
            :class="`ww-modal__resize-handle--${handle}`"
            @mousedown="onResizeStart(handle, $event)"
          ></div>
        </template>

        <!-- 头部 -->
        <div
          class="ww-modal__header"
          :class="[headerClass, { 'is-draggable': draggable && !isMaximized }]"
          @mousedown="onHeaderMousedown"
          @dblclick="toggleMaximize"
          @click="onMinimizedHeaderClick"
        >
          <div class="ww-modal__header-main">
            <slot name="title">
              <span class="ww-modal__title">{{ title }}</span>
            </slot>
            <span v-if="description && !isMinimized" class="ww-modal__description">
              {{ description }}
            </span>
          </div>
          <div class="ww-modal__actions" @mousedown.stop>
            <slot name="header-extra"></slot>
            <button
              v-if="minimizable"
              type="button"
              class="ww-modal__action"
              title="最小化"
              @click="minimize"
            >
              <svg viewBox="0 0 24 24" width="14" height="14" fill="none">
                <path d="M5 16.5h14" stroke="currentColor" stroke-width="2" stroke-linecap="round" />
              </svg>
            </button>
            <button
              v-if="maximizable && !isMinimized"
              type="button"
              class="ww-modal__action"
              :title="isMaximized ? '还原' : '最大化'"
              @click="toggleMaximize"
            >
              <!-- 最大化 -->
              <svg v-if="!isMaximized" viewBox="0 0 24 24" width="14" height="14" fill="none">
                <rect x="5" y="5" width="14" height="14" rx="1.5" stroke="currentColor" stroke-width="2" />
              </svg>
              <!-- 还原 -->
              <svg v-else viewBox="0 0 24 24" width="14" height="14" fill="none">
                <path d="M9 4.5h9A1.5 1.5 0 0 1 19.5 6v9" stroke="currentColor" stroke-width="2" stroke-linecap="round" />
                <rect x="4.5" y="8.5" width="11" height="11" rx="1.5" stroke="currentColor" stroke-width="2" />
              </svg>
            </button>
            <button
              v-if="closable"
              type="button"
              class="ww-modal__action ww-modal__action--close"
              title="关闭"
              @click="requestClose"
            >
              <svg viewBox="0 0 24 24" width="14" height="14" fill="none">
                <path d="M6 6l12 12M18 6L6 18" stroke="currentColor" stroke-width="2" stroke-linecap="round" />
              </svg>
            </button>
          </div>
        </div>

        <!-- 内容区 -->
        <div v-show="!isMinimized" class="ww-modal__body" :class="contentClass">
          <div v-if="loading" class="ww-modal__loading">
            <NSpin size="large" />
          </div>
          <template v-if="rendered">
            <div v-if="iframeSrc" class="ww-modal__iframe-wrap">
              <iframe
                ref="iframeRef"
                class="ww-modal__iframe"
                :src="iframeSrc"
                @load="onIframeLoad"
              ></iframe>
              <div v-if="!iframeLoaded" class="ww-modal__loading">
                <NSpin size="large" />
              </div>
            </div>
            <slot v-else></slot>
          </template>
        </div>

        <!-- 底部 -->
        <div
          v-if="footer && !isMinimized"
          class="ww-modal__footer"
          :class="footerClass"
        >
          <slot name="footer">
            <NButton v-if="showCancelButton" @click="onCancel">
              {{ cancelText }}
            </NButton>
            <NButton
              v-if="showConfirmButton"
              type="primary"
              :loading="confirmLoading"
              @click="onConfirm"
            >
              {{ confirmText }}
            </NButton>
          </slot>
        </div>
      </div>
    </Transition>
  </Teleport>
</template>

<style scoped>
.ww-modal__mask {
  position: fixed;
  inset: 0;
  background: rgb(0 0 0 / 45%);
}

.ww-modal {
  position: fixed;
  display: flex;
  flex-direction: column;
  overflow: hidden;
  background: var(--ww-modal-color, #fff);
  border-radius: var(--ww-modal-radius, 8px);
  box-shadow:
    0 3px 6px -4px rgb(0 0 0 / 12%),
    0 6px 16px rgb(0 0 0 / 8%),
    0 9px 28px 8px rgb(0 0 0 / 5%);
  color: var(--ww-modal-text-color, #333639);
}

.ww-modal.is-maximized {
  transition:
    left 0.2s ease,
    top 0.2s ease,
    width 0.2s ease,
    height 0.2s ease;
}

.ww-modal.is-dragging {
  user-select: none;
  cursor: move;
}

.ww-modal.is-dragging iframe {
  pointer-events: none;
}

.ww-modal.is-resizing iframe {
  pointer-events: none;
}

.ww-modal__header {
  display: flex;
  flex-shrink: 0;
  gap: 8px;
  align-items: center;
  justify-content: space-between;
  height: 45px;
  padding: 0 12px 0 16px;
  border-bottom: 1px solid var(--ww-modal-border-color, #efeff5);
}

.ww-modal.is-minimized .ww-modal__header {
  border-bottom: none;
  box-shadow: 0 2px 12px rgb(0 0 0 / 12%);
  cursor: pointer;
}

.ww-modal__header.is-draggable {
  cursor: move;
}

.ww-modal__header-main {
  display: flex;
  flex: 1;
  flex-direction: column;
  justify-content: center;
  overflow: hidden;
}

.ww-modal__title {
  overflow: hidden;
  font-size: 15px;
  font-weight: 600;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.ww-modal__description {
  overflow: hidden;
  font-size: 12px;
  color: var(--ww-modal-description-color, #909399);
  text-overflow: ellipsis;
  white-space: nowrap;
}

.ww-modal__actions {
  display: flex;
  flex-shrink: 0;
  gap: 2px;
  align-items: center;
}

.ww-modal__action {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 28px;
  height: 28px;
  cursor: pointer;
  border: none;
  border-radius: 6px;
  background: transparent;
  color: inherit;
  transition: background-color 0.15s ease;
}

.ww-modal__action:hover {
  background: var(--ww-modal-action-hover-color, rgb(0 0 0 / 6%));
}

.ww-modal__action--close:hover {
  background: #f56c6c;
  color: #fff;
}

.ww-modal__body {
  position: relative;
  display: flex;
  flex: 1;
  flex-direction: column;
  overflow: auto;
  padding: 16px;
}

.ww-modal.is-minimized .ww-modal__body {
  display: none;
}

.ww-modal__loading {
  position: absolute;
  inset: 0;
  z-index: 1;
  display: flex;
  align-items: center;
  justify-content: center;
  background: rgb(255 255 255 / 60%);
}

.ww-modal__iframe-wrap {
  position: relative;
  display: flex;
  flex: 1;
  flex-direction: column;
  min-height: 0;
}

.ww-modal__iframe {
  flex: 1;
  width: 100%;
  min-height: 200px;
  border: none;
}

.ww-modal__footer {
  display: flex;
  flex-shrink: 0;
  gap: 8px;
  align-items: center;
  justify-content: flex-end;
  padding: 10px 16px;
  border-top: 1px solid var(--ww-modal-border-color, #efeff5);
}

/* ---------- 缩放拖柄 ---------- */
.ww-modal__resize-handle {
  position: absolute;
  z-index: 10;
}

.ww-modal__resize-handle--n {
  top: -3px;
  left: 0;
  width: 100%;
  height: 6px;
  cursor: ns-resize;
}

.ww-modal__resize-handle--s {
  bottom: -3px;
  left: 0;
  width: 100%;
  height: 6px;
  cursor: ns-resize;
}

.ww-modal__resize-handle--e {
  top: 0;
  right: -3px;
  width: 6px;
  height: 100%;
  cursor: ew-resize;
}

.ww-modal__resize-handle--w {
  top: 0;
  left: -3px;
  width: 6px;
  height: 100%;
  cursor: ew-resize;
}

.ww-modal__resize-handle--ne {
  top: -3px;
  right: -3px;
  width: 12px;
  height: 12px;
  cursor: nesw-resize;
}

.ww-modal__resize-handle--nw {
  top: -3px;
  left: -3px;
  width: 12px;
  height: 12px;
  cursor: nwse-resize;
}

.ww-modal__resize-handle--se {
  right: -3px;
  bottom: -3px;
  width: 12px;
  height: 12px;
  cursor: nwse-resize;
}

.ww-modal__resize-handle--sw {
  bottom: -3px;
  left: -3px;
  width: 12px;
  height: 12px;
  cursor: nesw-resize;
}

/* ---------- 内置动画 ---------- */
/* 遮罩淡入淡出（所有动画共用） */
.ww-modal-mask-enter-active,
.ww-modal-mask-leave-active {
  transition: opacity var(--ww-modal-duration, 0.25s) ease;
}

.ww-modal-mask-enter-from,
.ww-modal-mask-leave-to {
  opacity: 0;
}

/* 缩放 + 淡入淡出（默认） */
.ww-modal-zoom-enter-active,
.ww-modal-zoom-leave-active {
  transition:
    opacity var(--ww-modal-duration, 0.25s) ease,
    transform var(--ww-modal-duration, 0.25s) ease;
}

.ww-modal-zoom-enter-from,
.ww-modal-zoom-leave-to {
  opacity: 0;
  transform: scale(0.85);
}

/* 纯淡入淡出 */
.ww-modal-fade-enter-active,
.ww-modal-fade-leave-active {
  transition: opacity var(--ww-modal-duration, 0.25s) ease;
}

.ww-modal-fade-enter-from,
.ww-modal-fade-leave-to {
  opacity: 0;
}

/* 自下向上滑入 */
.ww-modal-slide-up-enter-active,
.ww-modal-slide-up-leave-active {
  transition:
    opacity var(--ww-modal-duration, 0.25s) ease,
    transform var(--ww-modal-duration, 0.25s) ease;
}

.ww-modal-slide-up-enter-from,
.ww-modal-slide-up-leave-to {
  opacity: 0;
  transform: translateY(48px);
}

/* 自上向下滑入 */
.ww-modal-slide-down-enter-active,
.ww-modal-slide-down-leave-active {
  transition:
    opacity var(--ww-modal-duration, 0.25s) ease,
    transform var(--ww-modal-duration, 0.25s) ease;
}

.ww-modal-slide-down-enter-from,
.ww-modal-slide-down-leave-to {
  opacity: 0;
  transform: translateY(-48px);
}
</style>
