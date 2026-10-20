<template>
  <Teleport to="body">
    <Transition name="ww-modal-fade" @after-leave="onAfterLeave">
      <div
        v-if="show && !isMinimized"
        :class="['ww-modal-root', { 'ww-modal-maximized': isMaximized }]"
        :style="rootStyle"
        @mousedown="handleRootMousedown"
      >
        <div
          v-if="mask"
          class="ww-modal-mask"
          :style="{ zIndex: currentZIndex - 1 }"
          @click="handleMaskClick"
        />
        <div
          ref="panelRef"
          :class="['ww-modal-panel', { 'ww-dragging': isDragging }]"
          :style="panelStyle"
          @mousedown="handlePanelMousedown"
        >
          <div
            :class="['ww-modal-header', { 'ww-draggable': draggable && !isMaximized }]"
            :style="mergedHeaderStyle"
            @mousedown="handleHeaderMousedown"
            @dblclick="handleHeaderDblclick"
          >
            <div class="ww-modal-title" :style="{ fontSize: titleFontSize }">
              <slot name="title">{{ title }}</slot>
            </div>
            <div class="ww-modal-header-actions">
              <slot name="header-actions" />
              <button
                v-for="btn in extraButtons"
                :key="btn.key"
                class="ww-modal-btn"
                :title="btn.label"
                @click.stop="btn.onClick"
              >
                <svg v-if="btn.icon" :width="14" :height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                  <path v-for="(d, i) in getIconPaths(btn.icon)" :key="i" :d="d" />
                </svg>
                <span v-else>{{ btn.label }}</span>
              </button>
              <button v-if="minimizable" class="ww-modal-btn" title="最小化" @click.stop="handleMinimize">
                <svg :width="14" :height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M5 12h14" /></svg>
              </button>
              <button v-if="maximizable" class="ww-modal-btn" :title="isMaximized ? '还原' : '最大化'" @click.stop="toggleMaximize">
                <svg v-if="!isMaximized" :width="14" :height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="4" y="4" width="16" height="16" rx="1" /></svg>
                <svg v-else :width="14" :height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="7" width="14" height="14" rx="1" /><path d="M7 7V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2v10a2 2 0 0 1-2 2h-2" /></svg>
              </button>
              <button class="ww-modal-btn ww-modal-close" title="关闭" @click.stop="handleClose">
                <svg :width="14" :height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M18 6L6 18" /><path d="M6 6l12 12" /></svg>
              </button>
            </div>
          </div>

          <div class="ww-modal-body" :style="mergedBodyStyle">
            <slot>
              <iframe
                v-if="iframeSrc"
                :src="iframeSrc"
                class="ww-modal-iframe"
                :style="{ pointerEvents: isDragging || isResizing ? 'none' : 'auto' }"
                frameborder="0"
                allowfullscreen
              />
            </slot>
          </div>

          <div v-if="$slots.footer" class="ww-modal-footer">
            <slot name="footer" />
          </div>

          <template v-if="resizable && !isMaximized">
            <div class="ww-resize-handle ww-resize-n" @mousedown.stop="handleResizeStart('n', $event)" />
            <div class="ww-resize-handle ww-resize-s" @mousedown.stop="handleResizeStart('s', $event)" />
            <div class="ww-resize-handle ww-resize-e" @mousedown.stop="handleResizeStart('e', $event)" />
            <div class="ww-resize-handle ww-resize-w" @mousedown.stop="handleResizeStart('w', $event)" />
            <div class="ww-resize-handle ww-resize-ne" @mousedown.stop="handleResizeStart('ne', $event)" />
            <div class="ww-resize-handle ww-resize-nw" @mousedown.stop="handleResizeStart('nw', $event)" />
            <div class="ww-resize-handle ww-resize-se" @mousedown.stop="handleResizeStart('se', $event)" />
            <div class="ww-resize-handle ww-resize-sw" @mousedown.stop="handleResizeStart('sw', $event)" />
          </template>
        </div>
      </div>
    </Transition>
  </Teleport>
</template>

<script setup lang="ts">
import { ref, computed, watch, onMounted, onBeforeUnmount, nextTick, type CSSProperties, type StyleValue } from 'vue'
import { useModalZIndex } from '../composables/useModalZIndex'
import { registerMinimized, unregisterMinimized } from '../composables/useMinimizedRegistry'

interface ExtraButton {
  key: string
  icon?: string
  label?: string
  onClick: () => void
}

const props = withDefaults(
  defineProps<{
    show: boolean
    title?: string
    width?: string | number
    height?: string | number
    position?: 'center' | 'left' | 'right' | 'top' | 'bottom' | 'top-left' | 'top-right' | 'bottom-left' | 'bottom-right'
    mask?: boolean
    maskClosable?: boolean
    maximizable?: boolean
    minimizable?: boolean
    draggable?: boolean
    resizable?: boolean
    minWidth?: number
    minHeight?: number
    iframeSrc?: string
    padding?: string
    titleFontSize?: string
    headerStyle?: CSSProperties
    bodyStyle?: CSSProperties
    extraButtons?: ExtraButton[]
  }>(),
  {
    title: '',
    width: '520px',
    position: 'center',
    mask: true,
    maskClosable: false,
    maximizable: true,
    minimizable: true,
    draggable: true,
    resizable: false,
    minWidth: 300,
    minHeight: 200,
    iframeSrc: '',
    padding: '16px',
    titleFontSize: '16px',
    extraButtons: () => [],
  }
)

const emit = defineEmits<{
  (e: 'update:show', value: boolean): void
  (e: 'update:minimized', value: boolean): void
  (e: 'open'): void
  (e: 'close'): void
  (e: 'minimize'): void
  (e: 'restore'): void
  (e: 'maximize'): void
  (e: 'unmaximize'): void
}>()

const { generateId, register, unregister, bringToFront, getZIndex } = useModalZIndex()

const modalId = generateId()
const panelRef = ref<HTMLElement | null>(null)
const currentZIndex = ref(2000)

const isMaximized = ref(false)
const isMinimized = ref(false)
const isDragging = ref(false)
const isResizing = ref(false)

const panelLeft = ref(0)
const panelTop = ref(0)
const panelWidth = ref(0)
const panelHeight = ref(0)

const prevBounds = ref({ left: 0, top: 0, width: 0, height: 0 })

const ICON_PATHS: Record<string, string[]> = {
  refresh: ['M23 4v6h-6', 'M20.49 15a9 9 0 1 1-2.12-9.36L23 10'],
  plus: ['M12 5v14', 'M5 12h14'],
  trash: ['M3 6h18', 'M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6', 'M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2'],
  download: ['M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4', 'M7 10l5 5 5-5', 'M12 15V3'],
  eye: ['M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z', 'M15 12a3 3 0 1 1-6 0 3 3 0 0 1 6 0z'],
  search: ['M11 19a8 8 0 1 0 0-16 8 8 0 0 0 0 16z', 'M21 21l-4.35-4.35'],
}

function getIconPaths(name: string) {
  return ICON_PATHS[name] ?? []
}

function toPx(v: string | number | undefined, fallback: string): string {
  if (v == null) return fallback
  return typeof v === 'number' ? `${v}px` : v
}

function parsePx(v: string | number): number {
  if (typeof v === 'number') return v
  return parseFloat(v) || 0
}

const rootStyle = computed(() => ({
  zIndex: currentZIndex.value,
}))

const panelStyle = computed(() => {
  const s: CSSProperties = {
    zIndex: currentZIndex.value,
  }
  if (isMaximized.value) {
    s.left = '0'
    s.top = '0'
    s.width = '100vw'
    s.height = '100vh'
  } else {
    s.left = `${panelLeft.value}px`
    s.top = `${panelTop.value}px`
    s.width = panelWidth.value > 0 ? `${panelWidth.value}px` : toPx(props.width, '520px')
    if (props.height) {
      s.height = toPx(props.height, 'auto')
    }
  }
  return s
})

const mergedHeaderStyle = computed<StyleValue>(() => {
  return props.headerStyle ?? {}
})

const mergedBodyStyle = computed<StyleValue>(() => {
  return { padding: props.padding, ...(props.bodyStyle ?? {}) }
})

function calcInitialPosition() {
  const w = parsePx(props.width) || 520
  const vw = window.innerWidth
  const vh = window.innerHeight
  const margin = 16

  let left = 0
  let top = 0

  switch (props.position) {
    case 'center':
      left = (vw - w) / 2
      top = vh * 0.15
      break
    case 'left':
      left = margin
      top = (vh - 400) / 2
      break
    case 'right':
      left = vw - w - margin
      top = (vh - 400) / 2
      break
    case 'top':
      left = (vw - w) / 2
      top = margin
      break
    case 'bottom':
      left = (vw - w) / 2
      top = vh - 400 - margin
      break
    case 'top-left':
      left = margin
      top = margin
      break
    case 'top-right':
      left = vw - w - margin
      top = margin
      break
    case 'bottom-left':
      left = margin
      top = vh - 400 - margin
      break
    case 'bottom-right':
      left = vw - w - margin
      top = vh - 400 - margin
      break
  }

  panelLeft.value = Math.max(0, left)
  panelTop.value = Math.max(0, top)
  panelWidth.value = 0
  panelHeight.value = 0
}

watch(() => props.show, (val) => {
  if (val) {
    isMaximized.value = false
    isMinimized.value = false
    calcInitialPosition()
    try {
      register(modalId)
      currentZIndex.value = bringToFront(modalId)
    } catch (e) {
      console.warn('WwModal register error:', e)
    }
    emit('open')
  } else {
    try {
      unregister(modalId)
      unregisterMinimized(modalId)
    } catch (e) {
      console.warn('WwModal unregister error:', e)
    }
  }
})

function handleMaskClick() {
  if (props.maskClosable) {
    emit('update:show', false)
  }
}

function handleRootMousedown() {
  currentZIndex.value = bringToFront(modalId)
}

function handlePanelMousedown() {
  currentZIndex.value = bringToFront(modalId)
}

function handleClose() {
  emit('update:show', false)
  emit('close')
}

function handleMinimize() {
  isMinimized.value = true
  registerMinimized({
    id: modalId,
    title: props.title,
    restore: restoreFromMinimize,
  })
  emit('update:minimized', true)
  emit('minimize')
}

function restoreFromMinimize() {
  isMinimized.value = false
  unregisterMinimized(modalId)
  emit('update:minimized', false)
  emit('restore')
  currentZIndex.value = bringToFront(modalId)
}

function toggleMaximize() {
  if (isMaximized.value) {
    isMaximized.value = false
    panelLeft.value = prevBounds.value.left
    panelTop.value = prevBounds.value.top
    panelWidth.value = prevBounds.value.width
    panelHeight.value = prevBounds.value.height
    emit('unmaximize')
  } else {
    prevBounds.value = {
      left: panelLeft.value,
      top: panelTop.value,
      width: panelRef.value?.offsetWidth ?? parsePx(props.width),
      height: panelRef.value?.offsetHeight ?? 0,
    }
    isMaximized.value = true
    emit('maximize')
  }
}

function handleHeaderDblclick() {
  if (props.maximizable) toggleMaximize()
}

let dragStartX = 0
let dragStartY = 0
let dragStartLeft = 0
let dragStartTop = 0

function handleHeaderMousedown(e: MouseEvent) {
  if (!props.draggable || isMaximized.value) return
  e.preventDefault()
  isDragging.value = true
  dragStartX = e.clientX
  dragStartY = e.clientY
  dragStartLeft = panelLeft.value
  dragStartTop = panelTop.value
  document.addEventListener('mousemove', handleDragMove)
  document.addEventListener('mouseup', handleDragEnd)
}

function handleDragMove(e: MouseEvent) {
  const dx = e.clientX - dragStartX
  const dy = e.clientY - dragStartY
  const pw = panelRef.value?.offsetWidth ?? 520
  const ph = panelRef.value?.offsetHeight ?? 400
  const vw = window.innerWidth
  const vh = window.innerHeight

  panelLeft.value = Math.max(-pw + 100, Math.min(vw - 100, dragStartLeft + dx))
  panelTop.value = Math.max(0, Math.min(vh - 60, dragStartTop + dy))
}

function handleDragEnd() {
  isDragging.value = false
  document.removeEventListener('mousemove', handleDragMove)
  document.removeEventListener('mouseup', handleDragEnd)
}

let resizeDir = ''
let resizeStartX = 0
let resizeStartY = 0
let resizeStartLeft = 0
let resizeStartTop = 0
let resizeStartW = 0
let resizeStartH = 0

function handleResizeStart(dir: string, e: MouseEvent) {
  if (!props.resizable || isMaximized.value) return
  e.preventDefault()
  isResizing.value = true
  resizeDir = dir
  resizeStartX = e.clientX
  resizeStartY = e.clientY
  resizeStartLeft = panelLeft.value
  resizeStartTop = panelTop.value
  resizeStartW = panelRef.value?.offsetWidth ?? parsePx(props.width)
  resizeStartH = panelRef.value?.offsetHeight ?? 400
  document.addEventListener('mousemove', handleResizeMove)
  document.addEventListener('mouseup', handleResizeEnd)
}

function handleResizeMove(e: MouseEvent) {
  const dx = e.clientX - resizeStartX
  const dy = e.clientY - resizeStartY
  let newLeft = resizeStartLeft
  let newTop = resizeStartTop
  let newW = resizeStartW
  let newH = resizeStartH

  if (resizeDir.includes('e')) newW = resizeStartW + dx
  if (resizeDir.includes('w')) { newW = resizeStartW - dx; newLeft = resizeStartLeft + dx }
  if (resizeDir.includes('s')) newH = resizeStartH + dy
  if (resizeDir.includes('n')) { newH = resizeStartH - dy; newTop = resizeStartTop + dy }

  newW = Math.max(props.minWidth, newW)
  newH = Math.max(props.minHeight, newH)

  if (resizeDir.includes('w') && newW === props.minWidth) {
    newLeft = resizeStartLeft + resizeStartW - props.minWidth
  }
  if (resizeDir.includes('n') && newH === props.minHeight) {
    newTop = resizeStartTop + resizeStartH - props.minHeight
  }

  panelLeft.value = newLeft
  panelTop.value = newTop
  panelWidth.value = newW
  panelHeight.value = newH
}

function handleResizeEnd() {
  isResizing.value = false
  document.removeEventListener('mousemove', handleResizeMove)
  document.removeEventListener('mouseup', handleResizeEnd)
}

function onAfterLeave() {
  emit('close')
}

function handleKeydown(e: KeyboardEvent) {
  if (e.key === 'Escape' && props.show && !isMinimized.value) {
    handleClose()
  }
}

onMounted(() => {
  document.addEventListener('keydown', handleKeydown)
})

onBeforeUnmount(() => {
  unregister(modalId)
  unregisterMinimized(modalId)
  document.removeEventListener('keydown', handleKeydown)
  document.removeEventListener('mousemove', handleDragMove)
  document.removeEventListener('mouseup', handleDragEnd)
  document.removeEventListener('mousemove', handleResizeMove)
  document.removeEventListener('mouseup', handleResizeEnd)
})

defineExpose({
  restoreFromMinimize,
  toggleMaximize,
  bringToFront: () => { currentZIndex.value = bringToFront(modalId) },
})
</script>

<style scoped>
.ww-modal-root {
  position: fixed;
  inset: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  pointer-events: none;
}

.ww-modal-mask {
  position: fixed;
  inset: 0;
  background: var(--ww-mask);
  backdrop-filter: blur(2px);
  -webkit-backdrop-filter: blur(2px);
  pointer-events: auto;
}

.ww-modal-panel {
  position: fixed;
  display: flex;
  flex-direction: column;
  background: var(--ww-surface-modal);
  color: var(--ww-text-1);
  border: 1px solid var(--ww-border);
  border-radius: var(--ww-radius-lg);
  box-shadow: var(--ww-shadow-modal);
  pointer-events: auto;
  overflow: hidden;
  transition: box-shadow 0.2s ease, border-color 0.2s ease;
}

.ww-modal-panel.ww-dragging {
  user-select: none;
  box-shadow: var(--ww-shadow-modal), 0 0 0 1px var(--ww-primary-soft);
}

.ww-modal-maximized .ww-modal-panel {
  border-radius: 0;
  border-left: none;
  border-right: none;
}

.ww-modal-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  padding: 12px 14px 12px 18px;
  border-bottom: 1px solid var(--ww-border);
  background: var(--ww-surface-modal);
  flex-shrink: 0;
  user-select: none;
}

.ww-modal-header.ww-draggable {
  cursor: move;
}

.ww-modal-title {
  font-weight: 600;
  font-size: 15px;
  line-height: 22px;
  color: var(--ww-text-1);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  flex: 1;
  min-width: 0;
}

.ww-modal-header-actions {
  display: flex;
  align-items: center;
  gap: 2px;
  margin-left: 8px;
  flex-shrink: 0;
}

.ww-modal-btn {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 28px;
  height: 28px;
  border: none;
  background: transparent;
  border-radius: var(--ww-radius-sm);
  cursor: pointer;
  color: var(--ww-text-2);
  transition: background 0.15s ease, color 0.15s ease;
}

.ww-modal-btn:hover {
  background: var(--ww-surface-hover);
  color: var(--ww-text-1);
}

.ww-modal-close:hover {
  background: #e81123;
  border-color: #e81123;
  color: #fff;
}

.ww-modal-body {
  flex: 1;
  overflow: auto;
  min-height: 0;
  background: var(--ww-surface-modal);
  color: var(--ww-text-1);
}

.ww-modal-iframe {
  width: 100%;
  height: 100%;
  min-height: 300px;
  border: none;
}

.ww-modal-footer {
  display: flex;
  justify-content: flex-end;
  align-items: center;
  gap: 8px;
  padding: 12px 18px;
  border-top: 1px solid var(--ww-border);
  background: var(--ww-surface-modal);
  flex-shrink: 0;
}

.ww-resize-handle {
  position: absolute;
  z-index: 10;
}

.ww-resize-n { top: -3px; left: 6px; right: 6px; height: 6px; cursor: n-resize; }
.ww-resize-s { bottom: -3px; left: 6px; right: 6px; height: 6px; cursor: s-resize; }
.ww-resize-e { right: -3px; top: 6px; bottom: 6px; width: 6px; cursor: e-resize; }
.ww-resize-w { left: -3px; top: 6px; bottom: 6px; width: 6px; cursor: w-resize; }
.ww-resize-ne { top: -3px; right: -3px; width: 12px; height: 12px; cursor: ne-resize; }
.ww-resize-nw { top: -3px; left: -3px; width: 12px; height: 12px; cursor: nw-resize; }
.ww-resize-se { bottom: -3px; right: -3px; width: 12px; height: 12px; cursor: se-resize; }
.ww-resize-sw { bottom: -3px; left: -3px; width: 12px; height: 12px; cursor: sw-resize; }

.ww-modal-fade-enter-active,
.ww-modal-fade-leave-active {
  transition: opacity 0.2s ease;
}
.ww-modal-fade-enter-active .ww-modal-panel {
  animation: ww-modal-pop 0.22s cubic-bezier(0.22, 0.61, 0.36, 1);
}
.ww-modal-fade-enter-from,
.ww-modal-fade-leave-to {
  opacity: 0;
}

@keyframes ww-modal-pop {
  from {
    transform: translateY(8px) scale(0.98);
  }
  to {
    transform: translateY(0) scale(1);
  }
}
</style>
