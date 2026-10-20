<script setup lang="ts">
import { type ComponentPublicInstance, computed, nextTick, onBeforeUnmount, onMounted, reactive, ref } from 'vue'
import { NButton, NCard, NDatePicker, NInput, NModal, NSelect, NSpace, NVirtualList, dateZhCN, useMessage, zhCN } from 'naive-ui'
import {
  deleteLogsByDateRangeApi,
  getLogPageApi,
  LOG_LEVELS,
  startLogStreamApi,
  type LogItem,
  type LogStreamHandle
} from '@/api/log'
import { getClusterNodesApi } from '@/api/cluster'

defineOptions({ name: 'LogManage' })

const message = useMessage()

// ===== 清除日志 =====
const showCleanup = ref(false)
const cleanupRange = ref<[number, number] | null>(null)
const cleanupLoading = ref(false)

function presetHours(hours: number) {
  const now = Date.now()
  cleanupRange.value = [now - hours * 3600_000, now]
}

async function handleCleanup() {
  if (!cleanupRange.value) {
    message.warning('请选择时间范围')
    return
  }
  const [from, to] = cleanupRange.value
  if (from >= to) {
    message.warning('起始时间必须早于结束时间')
    return
  }
  cleanupLoading.value = true
  try {
    const res = await deleteLogsByDateRangeApi(from, to)
    message.success(`已清除 ${res.deleted} 条日志`)
    showCleanup.value = false
    cleanupRange.value = null
    onServerFilterChange()
  } catch (e: any) {
    message.error(e?.message || '清除失败')
  } finally {
    cleanupLoading.value = false
  }
}

// ===== 常量 =====
const MAX_ROWS = 5000
const PAGE_SIZE = 200
const FLUSH_INTERVAL = 300
const SEEN_CAP = 4096
const RECONNECT_BASE_MS = 2000
const RECONNECT_MAX_MS = 15000

// ===== 过滤 =====
const levelOptions = LOG_LEVELS.map(l => ({ label: l, value: l }))
const nodeOptions = ref<Array<{ label: string; value: string }>>([])

const filter = reactive({
  level: null as null | string,
  node: null as null | string,
  keyword: ''
})

let keywordTimer: ReturnType<typeof setTimeout> | null = null
const keywordInput = ref('')

function onKeywordInput() {
  if (keywordTimer) clearTimeout(keywordTimer)
  keywordTimer = setTimeout(() => {
    if (keywordInput.value !== filter.keyword) {
      filter.keyword = keywordInput.value
      onServerFilterChange()
    }
  }, 400)
}

function onServerFilterChange() {
  disconnectStream()
  rows.value = []
  seen.value = new Set()
  pending.value = []
  windowState.hasMore = true
  fetchInitial().then(() => connectStream())
}

function onReset() {
  filter.level = null
  filter.node = null
  filter.keyword = ''
  keywordInput.value = ''
  onServerFilterChange()
}

// ===== 数据 =====
const rows = ref<LogItem[]>([])
const seen = ref(new Set<string>())
const pending = ref<LogItem[]>([])
const pinned = ref(true)
const pendingNew = ref(0)
const historyMemoryMode = ref(false)

const windowState = reactive({
  hasMore: true,
  loading: false
})

let streamHandle: LogStreamHandle | null = null
let flushTimer: ReturnType<typeof setInterval> | null = null
let reconnectTimer: ReturnType<typeof setTimeout> | null = null
let reconnectDelay = RECONNECT_BASE_MS
let streamConnected = ref(false)

const vlistWrapperRef = ref<ComponentPublicInstance | null>(null)
let scrollEl: HTMLElement | null = null
let isScrollingProgrammatic = false

function resolveScrollEl(): HTMLElement | null {
  const root = vlistWrapperRef.value?.$el as HTMLElement | null | undefined
  if (!root) return null
  if (isScrollContainer(root)) return root
  for (let i = 0; i < root.children.length; i++) {
    const child = root.children[i] as HTMLElement
    if (isScrollContainer(child)) return child
  }
  return findScrollChild(root)
}

function isScrollContainer(el: HTMLElement): boolean {
  const oy = getComputedStyle(el).overflowY
  return oy === 'auto' || oy === 'scroll'
}

function findScrollChild(el: HTMLElement): HTMLElement | null {
  for (let i = 0; i < el.children.length; i++) {
    const child = el.children[i] as HTMLElement
    if (isScrollContainer(child)) return child
    const found = findScrollChild(child)
    if (found) return found
  }
  return null
}

function ensureScrollEl(): HTMLElement | null {
  if (scrollEl && scrollEl.isConnected) return scrollEl
  scrollEl = resolveScrollEl()
  return scrollEl
}

function smoothScrollToBottom() {
  const el = ensureScrollEl()
  if (!el) return
  isScrollingProgrammatic = true
  el.scrollTo({ top: el.scrollHeight, behavior: 'smooth' })
  setTimeout(() => { isScrollingProgrammatic = false }, 350)
}

function instantScrollToBottom() {
  const el = ensureScrollEl()
  if (!el) return
  isScrollingProgrammatic = true
  el.scrollTop = el.scrollHeight
  setTimeout(() => { isScrollingProgrammatic = false }, 50)
}

// ===== 工具函数 =====
function uid(log: LogItem): string {
  if (log.id != null) return `db-${log.id}`
  const short = log.message.length > 40 ? log.message.slice(0, 40) : log.message
  return `live-${log.timestamp}-${log.node}-${short}`
}

function trackSeen(id: string) {
  seen.value.add(id)
  if (seen.value.size > SEEN_CAP) {
    const it = seen.value.values()
    const toRemove = seen.value.size - SEEN_CAP
    for (let i = 0; i < toRemove; i++) {
      seen.value.delete(it.next().value!)
    }
  }
}

function formatTime(ts?: number): string {
  if (!ts) return ''
  const d = new Date(ts)
  const p = (n: number) => String(n).padStart(2, '0')
  const ms = String(d.getMilliseconds()).padStart(3, '0')
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}.${ms}`
}

function padLevel(level: string): string {
  return level.padEnd(5)
}

function displayMessage(msg: string): string {
  return msg.replace(/^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}[.,]\d{1,3}\s+\|\s*/, '')
}

const keywordPredicate = (log: LogItem, kw: string): boolean => {
  const lower = kw.toLowerCase()
  return (
    log.message.toLowerCase().includes(lower) ||
    log.loggerName.toLowerCase().includes(lower) ||
    log.node.toLowerCase().includes(lower) ||
    (log.stackTrace?.toLowerCase().includes(lower) ?? false)
  )
}

const filteredRows = computed(() => {
  const kw = filter.keyword.trim()
  if (!kw) return rows.value
  return rows.value.filter(r => keywordPredicate(r, kw))
})

// ===== 历史加载 =====
async function fetchInitial() {
  windowState.loading = true
  try {
    const res = await getLogPageApi({
      pageNumber: 1,
      pageSize: PAGE_SIZE,
      level: filter.level ?? undefined,
      node: filter.node ?? undefined,
      keyword: filter.keyword.trim() || undefined
    })
    const reversed = [...res.logs].reverse()
    for (const log of reversed) {
      const id = uid(log)
      if (!seen.value.has(id)) {
        trackSeen(id)
      }
    }
    rows.value = reversed
    windowState.hasMore = res.hasMore ?? (res.logs.length >= PAGE_SIZE)
    historyMemoryMode.value = false
  } catch (e: any) {
    if (e?.status === 503 || e?.response?.status === 503) {
      historyMemoryMode.value = true
      windowState.hasMore = false
    }
    rows.value = []
  } finally {
    windowState.loading = false
  }
}

async function loadOlder() {
  if (windowState.loading || !windowState.hasMore) return
  const oldest = rows.value.find(r => r.id != null)
  if (!oldest || oldest.id == null) return

  windowState.loading = true
  const oldScrollTop = scrollEl?.scrollTop ?? 0
  const oldScrollHeight = scrollEl?.scrollHeight ?? 0

  try {
    const res = await getLogPageApi({
      pageSize: PAGE_SIZE,
      beforeTs: oldest.timestamp,
      beforeId: oldest.id,
      level: filter.level ?? undefined,
      node: filter.node ?? undefined,
      keyword: filter.keyword.trim() || undefined
    })
    const older = [...res.logs].reverse()
    const deduped = older.filter(log => {
      const id = uid(log)
      if (seen.value.has(id)) return false
      trackSeen(id)
      return true
    })
    rows.value = [...deduped, ...rows.value]
    if (rows.value.length > MAX_ROWS) {
      rows.value = rows.value.slice(rows.value.length - MAX_ROWS)
    }
    windowState.hasMore = res.hasMore ?? (res.logs.length >= PAGE_SIZE)

    await nextTick()
    if (scrollEl) {
      const newScrollHeight = scrollEl.scrollHeight
      scrollEl.scrollTop = oldScrollTop + (newScrollHeight - oldScrollHeight)
    }
  } catch {
    // ignore
  } finally {
    windowState.loading = false
  }
}

// ===== 实时流 =====
function connectStream() {
  disconnectStream()
  streamConnected.value = false
  reconnectDelay = RECONNECT_BASE_MS

  streamHandle = startLogStreamApi(
    {
      level: filter.level ?? undefined,
      node: filter.node ?? undefined
    },
    (log) => {
      reconnectDelay = RECONNECT_BASE_MS
      const id = uid(log)
      if (seen.value.has(id)) return
      trackSeen(id)
      pending.value.push(log)
    },
    () => {
      streamConnected.value = false
      scheduleReconnect()
    },
    () => {
      streamConnected.value = true
    }
  )

  startFlushLoop()
}

function disconnectStream() {
  streamHandle?.stop()
  streamHandle = null
  streamConnected.value = false
  if (reconnectTimer) {
    clearTimeout(reconnectTimer)
    reconnectTimer = null
  }
  stopFlushLoop()
}

function scheduleReconnect() {
  if (reconnectTimer) return
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null
    connectStream()
  }, reconnectDelay)
  reconnectDelay = Math.min(reconnectDelay * 1.5, RECONNECT_MAX_MS)
}

function startFlushLoop() {
  stopFlushLoop()
  flushTimer = setInterval(flushPending, FLUSH_INTERVAL)
}

function stopFlushLoop() {
  if (flushTimer) {
    clearInterval(flushTimer)
    flushTimer = null
  }
}

function flushPending() {
  if (pending.value.length === 0) return
  const batch = pending.value.sort((a, b) => a.timestamp - b.timestamp)
  pending.value = []

  const deduped = batch.filter(log => {
    const id = uid(log)
    return !rows.value.some(r => uid(r) === id)
  })
  if (deduped.length === 0) return

  rows.value = [...rows.value, ...deduped]
  if (rows.value.length > MAX_ROWS) {
    rows.value = rows.value.slice(rows.value.length - MAX_ROWS)
  }

  if (pinned.value) {
    nextTick(() => smoothScrollToBottom())
  } else {
    pendingNew.value += deduped.length
  }
}

function goLive() {
  pinned.value = true
  pendingNew.value = 0
  nextTick(() => smoothScrollToBottom())
}

// ===== 滚动监听 =====
function onNativeScroll() {
  if (isScrollingProgrammatic || !scrollEl) return

  const atBottom = scrollEl.scrollHeight - scrollEl.scrollTop - scrollEl.clientHeight < 40
  pinned.value = atBottom
  if (atBottom) pendingNew.value = 0

  if (scrollEl.scrollTop < 80 && !windowState.loading && windowState.hasMore) {
    loadOlder()
  }
}

// ===== 节点列表 =====
async function fetchNodeOptions() {
  try {
    const nodes = await getClusterNodesApi()
    nodeOptions.value = nodes.map(n => ({ label: n.nodeId, value: n.nodeId }))
  } catch {
    // ignore
  }
}

// ===== 生命周期 =====
onMounted(() => {
  fetchNodeOptions()
  fetchInitial().then(() => {
    nextTick(() => {
      scrollEl = resolveScrollEl()
      if (scrollEl) {
        scrollEl.addEventListener('scroll', onNativeScroll, { passive: true })
      }
      instantScrollToBottom()
      connectStream()
    })
  })
})

onBeforeUnmount(() => {
  disconnectStream()
  if (keywordTimer) clearTimeout(keywordTimer)
  if (scrollEl) scrollEl.removeEventListener('scroll', onNativeScroll)
})

// ===== NVirtualList 需要的 key 映射 =====
const listItems = computed(() =>
  filteredRows.value.map(log => ({ ...log, key: uid(log) }))
)
</script>

<template>
  <div class="log-page">
    <!-- 过滤栏 -->
    <div class="log-toolbar">
      <NSpace :wrap="true" :size="[8, 8]" align="center">
        <NSelect
          v-model:value="filter.level"
          :options="levelOptions"
          placeholder="日志级别"
          clearable
          style="width: 130px"
          @update:value="onServerFilterChange"
        />
        <NSelect
          v-model:value="filter.node"
          :options="nodeOptions"
          filterable
          tag
          placeholder="节点名"
          clearable
          style="width: 180px"
          @update:value="onServerFilterChange"
        />
        <NInput
          v-model:value="keywordInput"
          placeholder="关键词"
          clearable
          style="width: 220px"
          @input="onKeywordInput"
          @keyup.enter="() => { filter.keyword = keywordInput; onServerFilterChange() }"
        />
        <NButton size="small" @click="onServerFilterChange">查询</NButton>
        <NButton size="small" @click="onReset">重置</NButton>
      </NSpace>
      <div class="log-toolbar-right">
        <NButton size="small" type="error" ghost @click="showCleanup = true">清除日志</NButton>
        <span class="stream-dot" :class="streamConnected ? 'dot-live' : 'dot-off'"></span>
        <span class="stream-label">{{ streamConnected ? '实时推流中' : '未连接' }}</span>
      </div>
    </div>

    <!-- memory 模式提示 -->
    <div v-if="historyMemoryMode" class="memory-banner">
      历史查询不可用（memory 模式），仅显示实时日志
    </div>

    <!-- 日志控制台 -->
    <div class="log-console-wrap">
      <NVirtualList
        ref="vlistWrapperRef"
        class="log-console"
        :items="listItems"
        :item-size="22"
        item-resizable
        key-field="key"
      >
        <template #default="{ item }">
          <div class="log-line" :class="'level-' + item.level.toLowerCase()">
            <!-- 字段间隔使用真实空格文本（父级 white-space: pre-wrap 会保留），
                 而非 margin——CSS 间距不会进入剪贴板，此前复制日志时各字段粘连 -->
            <span class="lg-ts">{{ formatTime(item.timestamp) }}{{ ' ' }}</span>
            <span class="lg-level" :class="'lg-level-' + item.level.toLowerCase()">{{ padLevel(item.level) }}{{ ' ' }}</span>
            <span class="lg-node">{{ item.node }}</span>{{ ' ' }}
            <span class="lg-logger">{{ item.loggerName }}{{ ' ' }}</span>
            <span v-if="item.threadName" class="lg-thread">[{{ item.threadName }}]{{ ' ' }}</span>
            <span class="lg-msg">{{ displayMessage(item.message) }}</span>
            <pre v-if="item.stackTrace" class="lg-stack" :class="item.level === 'ERROR' ? 'stack-error' : ''">{{ item.stackTrace }}</pre>
          </div>
        </template>
      </NVirtualList>

      <!-- 浮动：加载更早 -->
      <button
        v-if="windowState.hasMore && !windowState.loading"
        class="float-btn float-load-older"
        @click="loadOlder"
      >
        ↑ 加载更早的日志
      </button>
      <div v-if="windowState.loading" class="float-btn float-loading">加载中...</div>

      <!-- 浮动：跳转到最新 -->
      <button
        v-if="!pinned"
        class="float-btn float-bottom"
        @click="goLive"
      >
        ↓ {{ pendingNew > 0 ? pendingNew + ' 条新日志' : '跳转到最新' }}
      </button>
    </div>

    <!-- 清除日志弹窗 -->
    <NModal v-model:show="showCleanup">
      <NCard title="清除日志" style="width: 440px" :bordered="false" size="small" role="dialog">
        <div class="cleanup-body">
          <div class="cleanup-row">
            <span class="cleanup-label">时间范围</span>
            <NDatePicker v-model:value="cleanupRange" type="datetimerange" clearable
                         :locale="zhCN" :date-locale="dateZhCN"
                         style="width: 100%" />
          </div>
          <div class="cleanup-presets">
            <NButton size="tiny" @click="presetHours(1)">近 1 小时</NButton>
            <NButton size="tiny" @click="presetHours(24)">近 24 小时</NButton>
            <NButton size="tiny" @click="presetHours(168)">近 7 天</NButton>
            <NButton size="tiny" @click="presetHours(720)">近 30 天</NButton>
          </div>
          <div class="cleanup-hint">
            将永久删除所选时间范围内的所有日志，操作不可撤销。
          </div>
        </div>
        <template #footer>
          <div style="display: flex; justify-content: flex-end; gap: 8px">
            <NButton size="small" @click="showCleanup = false">取消</NButton>
            <NButton size="small" type="error" :loading="cleanupLoading" @click="handleCleanup">确认清除</NButton>
          </div>
        </template>
      </NCard>
    </NModal>
  </div>
</template>

<style scoped>
.log-page {
  display: flex;
  flex-direction: column;
  height: 100%;
  gap: 8px;
}

.log-toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  flex-shrink: 0;
}

.log-toolbar-right {
  display: flex;
  align-items: center;
  gap: 6px;
}

.stream-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  display: inline-block;
}

.dot-live {
  background: #4ade80;
  box-shadow: 0 0 4px #4ade80;
}

.dot-off {
  background: #6b7280;
}

.stream-label {
  font-size: 12px;
  color: var(--ww-text-3);
}

.memory-banner {
  background: #fef3c7;
  color: #92400e;
  border: 1px solid #fcd34d;
  padding: 7px 12px;
  border-radius: var(--ww-radius);
  font-size: 13px;
  flex-shrink: 0;
}

:global(html.dark) .memory-banner {
  background: rgba(240, 160, 32, 0.14);
  border-color: rgba(240, 160, 32, 0.32);
  color: #f0a020;
}

.log-console-wrap {
  flex: 1;
  min-height: 0;
  position: relative;
  border-radius: var(--ww-radius);
  overflow: hidden;
  border: 1px solid #30363d;
}

.log-console {
  height: 100%;
  background: #0d1117;
  color: #e6edf3;
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 12px;
  line-height: 20px;
  padding: 4px 0;
}

.log-line {
  padding: 1px 12px;
  white-space: pre-wrap;
  word-break: break-all;
}

.log-line.level-error {
  background: rgba(248, 113, 113, 0.08);
}

.log-line.level-warn {
  background: rgba(251, 191, 36, 0.06);
}

.lg-ts {
  color: #7d8590;
}

.lg-level {
  font-weight: 600;
}

.lg-level-error {
  color: #f87171;
}

.lg-level-warn {
  color: #fbbf24;
}

.lg-level-info {
  color: #4ade80;
}

.lg-level-debug {
  color: #9ca3af;
}

.lg-level-trace {
  color: #6b7280;
}

.lg-node {
  background: #1f2937;
  color: #93c5fd;
  /* 仅保留徽标内部留白（色块自身造型）；字段间间隔由真实空格承担，保证可复制 */
  padding: 0 4px;
  border-radius: 3px;
  font-size: 11px;
}

.lg-logger {
  color: #22d3ee;
}

.lg-thread {
  color: #6e7681;
}

.lg-msg {
  color: #e6edf3;
}

.lg-stack {
  margin: 2px 0 2px 0;
  padding: 4px 8px;
  border-left: 2px solid #30363d;
  color: #8b949e;
  font-size: 11px;
  white-space: pre-wrap;
  word-break: break-all;
}

.lg-stack.stack-error {
  border-left-color: #f87171;
  color: #f0a8a8;
}

.float-btn {
  position: absolute;
  padding: 4px 12px;
  border-radius: 12px;
  font-size: 12px;
  border: 1px solid #30363d;
  cursor: pointer;
  z-index: 10;
  background: #161b22;
  color: #c9d1d9;
  transition: background 0.15s;
}

.float-btn:hover {
  background: #21262d;
}

.float-load-older {
  top: 8px;
  left: 50%;
  transform: translateX(-50%);
}

.float-loading {
  top: 8px;
  left: 50%;
  transform: translateX(-50%);
  cursor: default;
  opacity: 0.7;
}

.float-bottom {
  bottom: 8px;
  left: 50%;
  transform: translateX(-50%);
  background: #1f6feb;
  border-color: #1f6feb;
  color: #fff;
  animation: float-in 0.2s ease-out;
}

.float-bottom:hover {
  background: #388bfd;
}

@keyframes float-in {
  from {
    opacity: 0;
    transform: translateX(-50%) translateY(8px);
  }
  to {
    opacity: 1;
    transform: translateX(-50%) translateY(0);
  }
}

.cleanup-body {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.cleanup-row {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.cleanup-label {
  font-size: 13px;
  color: var(--ww-text-2);
}

.cleanup-presets {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
}

.cleanup-hint {
  font-size: 12px;
  color: var(--ww-text-3);
  line-height: 1.6;
}
</style>
