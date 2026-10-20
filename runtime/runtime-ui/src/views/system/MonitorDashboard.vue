<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, reactive, ref, watch } from 'vue'
import { NButton, NCard, NRadioButton, NRadioGroup, NTag, NTooltip } from 'naive-ui'
import * as echarts from 'echarts/core'
import { LineChart } from 'echarts/charts'
import { GridComponent, LegendComponent, TooltipComponent } from 'echarts/components'
import { CanvasRenderer } from 'echarts/renderers'
import type { ECharts, EChartsCoreOption } from 'echarts/core'
import IconPause from '~icons/mdi/pause'
import IconPlay from '~icons/mdi/play'
import IconRefresh from '~icons/mdi/refresh'
import { startRequestRateStreamApi, type MonitorStreamHandle, type RequestRateSnapshot } from '@/api/monitor'
import { useTheme } from '@/composables/useTheme'

echarts.use([LineChart, GridComponent, TooltipComponent, LegendComponent, CanvasRenderer])

defineOptions({ name: 'MonitorDashboard' })

const { isDark } = useTheme()

// ------------------------------------------------------------------
// 实时数据模型（最长保留 15 分钟，切换时间窗口不丢历史点）
// ------------------------------------------------------------------

/** Grafana classic palette：按节点首次出现顺序稳定分配，节点消亡也不回收，避免颜色串位 */
const PALETTE = [
  '#7EB26D', '#EAB839', '#6ED0E0', '#EF843C', '#E24D42',
  '#1F78C1', '#BA43A9', '#705DA0', '#508642', '#CCA300',
  '#447EBC', '#C15C17', '#890F02', '#0A437C', '#6D1F62', '#584477'
]
const MAX_WINDOW_MS = 15 * 60 * 1000
/** 节点持续多少秒无数据即在明细表标记为离线 */
const OFFLINE_AFTER_MS = 5000

interface NodeSeries {
  nodeId: string
  color: string
  state: string
  current: number | null
  lastSeenTs: number
  points: Array<[number, number | null]>
}

const seriesMap = reactive(new Map<string, NodeSeries>())
const totals = ref<Array<{ ts: number; total: number }>>([])
const frameReceived = ref(false)
const frameNodeCount = ref(0)
const lastFrameTs = ref(0)

const connected = ref(false)
const connecting = ref(true)
const paused = ref(false)

const windowMinutes = ref(5)
const windowMs = computed(() => windowMinutes.value * 60 * 1000)

let colorCursor = 0
function nextColor(): string {
  const color = PALETTE[colorCursor % PALETTE.length]
  colorCursor++
  return color
}

function applyFrame(snap: RequestRateSnapshot) {
  connecting.value = false
  if (paused.value) return
  const ts = snap.timestamp
  // 重连后可能收到旧时间戳帧，忽略防止时间轴回退
  if (ts < lastFrameTs.value) return

  frameReceived.value = true
  frameNodeCount.value = snap.nodes.length
  lastFrameTs.value = ts

  const seen = new Set<string>()
  let frameTotal = 0
  for (const node of snap.nodes) {
    seen.add(node.nodeId)
    let series = seriesMap.get(node.nodeId)
    if (!series) {
      series = {
        nodeId: node.nodeId,
        color: nextColor(),
        state: node.state,
        current: node.rps,
        lastSeenTs: ts,
        points: []
      }
      seriesMap.set(node.nodeId, series)
    }
    series.state = node.state
    if (node.rps !== null) {
      series.current = node.rps
      series.lastSeenTs = ts
      frameTotal += node.rps
    } else {
      series.current = null
    }
    const lastPoint = series.points[series.points.length - 1]
    if (lastPoint && lastPoint[0] === ts) {
      lastPoint[1] = node.rps
    } else {
      series.points.push([ts, node.rps])
    }
    // 按最大窗口裁剪，内存有界
    const cutoff = ts - MAX_WINDOW_MS
    while (series.points.length > 0 && series.points[0]![0] < cutoff) {
      series.points.shift()
    }
  }

  // 帧中消失的节点（DEAD/被注册表摘除）：在当前时刻打缺口，旧线断开而非连成 0
  for (const series of seriesMap.values()) {
    if (!seen.has(series.nodeId)) {
      const lastPoint = series.points[series.points.length - 1]
      if (!lastPoint || lastPoint[0] !== ts) {
        series.points.push([ts, null])
      }
      series.current = null
    }
  }

  totals.value.push({ ts, total: frameTotal })
  const cutoff = ts - MAX_WINDOW_MS
  totals.value = totals.value.filter((p) => p.ts >= cutoff)

  renderChart()
}

// ------------------------------------------------------------------
// 统计
// ------------------------------------------------------------------

const currentTotal = computed(() => {
  const last = totals.value[totals.value.length - 1]
  return last ? last.total : 0
})

const visibleTotals = computed(() => {
  const cutoff = lastFrameTs.value - windowMs.value
  return totals.value.filter((p) => p.ts >= cutoff)
})

const peakTotal = computed(() =>
  visibleTotals.value.reduce((max, p) => Math.max(max, p.total), 0)
)

const avgTotal = computed(() => {
  if (visibleTotals.value.length === 0) return 0
  const sum = visibleTotals.value.reduce((acc, p) => acc + p.total, 0)
  return Math.round((sum / visibleTotals.value.length) * 10) / 10
})

const nodeRows = computed(() =>
  Array.from(seriesMap.values()).map((s) => {
    const cutoff = lastFrameTs.value - windowMs.value
    const pts = s.points.filter((p) => p[0] >= cutoff)
    const values = pts.map((p) => p[1]).filter((v): v is number => v !== null)
    const avg = values.length ? values.reduce((a, b) => a + b, 0) / values.length : 0
    const max = values.length ? Math.max(...values) : 0
    const offline = lastFrameTs.value - s.lastSeenTs > OFFLINE_AFTER_MS
    return {
      nodeId: s.nodeId,
      color: s.color,
      state: s.state,
      current: s.current,
      avg: Math.round(avg * 10) / 10,
      max,
      offline
    }
  })
)

const lastFrameTimeText = computed(() => {
  if (!lastFrameTs.value) return '--:--:--'
  const d = new Date(lastFrameTs.value)
  return d.toLocaleTimeString('zh-CN', { hour12: false })
})

// ------------------------------------------------------------------
// ECharts
// ------------------------------------------------------------------

const chartEl = ref<HTMLDivElement | null>(null)
let chart: ECharts | null = null
let resizeObserver: ResizeObserver | null = null

function hexToRgba(hex: string, alpha: number): string {
  const r = parseInt(hex.slice(1, 3), 16)
  const g = parseInt(hex.slice(3, 5), 16)
  const b = parseInt(hex.slice(5, 7), 16)
  return `rgba(${r},${g},${b},${alpha})`
}

function buildOption(): EChartsCoreOption {
  const dark = isDark.value
  const text = dark ? '#c9cbd1' : '#4a5058'
  const subText = dark ? '#8b8e95' : '#8a8f99'
  const gridLine = dark ? 'rgba(255,255,255,0.08)' : 'rgba(0,0,0,0.07)'
  const tooltipBg = dark ? '#1b1d22' : '#ffffff'
  const axisLine = dark ? 'rgba(255,255,255,0.18)' : 'rgba(0,0,0,0.18)'

  const seriesList = Array.from(seriesMap.values())
  const cutoff = lastFrameTs.value - windowMs.value

  const series = seriesList.map((s) => ({
    name: s.nodeId,
    type: 'line' as const,
    // 平滑折线：轻度样条平滑，保留尖峰特征（Grafana 观感）
    smooth: 0.3,
    showSymbol: false,
    symbol: 'circle',
    symbolSize: 7,
    lineStyle: { width: 2, color: s.color },
    itemStyle: { color: s.color },
    // 悬停时高亮当前序列、弱化其余
    emphasis: { focus: 'series' as const, lineStyle: { width: 2.5 } },
    // 极淡面积渐变，多节点重叠不糊
    areaStyle: {
      color: {
        type: 'linear' as const,
        x: 0, y: 0, x2: 0, y2: 1,
        colorStops: [
          { offset: 0, color: hexToRgba(s.color, dark ? 0.16 : 0.12) },
          { offset: 1, color: hexToRgba(s.color, 0) }
        ]
      }
    },
    // 缺口不连线：对端瞬断/节点死亡保持 null 缺口语义
    connectNulls: false,
    data: s.points.filter((p) => p[0] >= cutoff)
  }))

  return {
    backgroundColor: 'transparent',
    // 每秒全量刷新，关闭动画避免折线抖动
    animation: false,
    color: seriesList.map((s) => s.color),
    grid: { left: 54, right: 22, top: 38, bottom: 34, containLabel: false },
    legend: {
      show: seriesList.length > 0,
      top: 2,
      left:  'right',
      right: 22,
      icon: 'roundRect',
      itemWidth: 16,
      itemHeight: 3,
      itemGap: 18,
      type: 'scroll',
      textStyle: { color: text, fontSize: 12 },
      inactiveColor: subText,
      data: seriesList.map((s) => s.nodeId)
    },
    tooltip: {
      trigger: 'axis',
      backgroundColor: tooltipBg,
      borderColor: axisLine,
      borderWidth: 1,
      padding: [8, 12],
      textStyle: { color: text, fontSize: 12 },
      axisPointer: {
        type: 'line',
        lineStyle: { color: subText, type: 'dashed', width: 1 }
      },
      valueFormatter: (value: unknown) =>
        value === null || value === undefined ? '-' : `${value} req/s`
    },
    xAxis: {
      type: 'time',
      min: lastFrameTs.value ? cutoff : undefined,
      max: lastFrameTs.value ? lastFrameTs.value + 1000 : undefined,
      boundaryGap: false,
      axisLine: { lineStyle: { color: axisLine } },
      axisTick: { show: false },
      axisLabel: {
        color: subText,
        fontSize: 11,
        hideOverlap: true,
        formatter: (value: number) =>
          new Date(value).toLocaleTimeString('zh-CN', { hour12: false })
      },
      splitLine: { show: false }
    },
    yAxis: {
      type: 'value',
      name: 'req/s',
      nameTextStyle: { color: subText, fontSize: 11, padding: [0, 0, 0, -40] },
      minInterval: 1,
      scale: true,
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: { color: subText, fontSize: 11 },
      splitLine: { lineStyle: { color: gridLine, type: 'dashed' } }
    },
    series
  }
}

function renderChart() {
  if (!chart) return
  chart.setOption(buildOption())
}

// ------------------------------------------------------------------
// SSE 生命周期
// ------------------------------------------------------------------

let streamHandle: MonitorStreamHandle | null = null

function connect() {
  connecting.value = true
  streamHandle = startRequestRateStreamApi(
    (snap) => applyFrame(snap),
    (ok) => {
      connected.value = ok
      if (ok) connecting.value = false
    }
  )
}

function togglePause() {
  paused.value = !paused.value
}

function reconnectNow() {
  streamHandle?.stop()
  connect()
}

onMounted(() => {
  if (chartEl.value) {
    chart = echarts.init(chartEl.value, undefined, { renderer: 'canvas' })
    resizeObserver = new ResizeObserver(() => chart?.resize())
    resizeObserver.observe(chartEl.value)
  }
  connect()
})

onBeforeUnmount(() => {
  streamHandle?.stop()
  streamHandle = null
  resizeObserver?.disconnect()
  resizeObserver = null
  chart?.dispose()
  chart = null
})

// 主题/窗口切换重绘
watch(isDark, () => renderChart())
watch(windowMinutes, () => renderChart())

// ------------------------------------------------------------------
// 展示样式
// ------------------------------------------------------------------

const statusMeta = computed(() => {
  if (paused.value) {
    return { dot: '#f0a020', text: '已暂停', pulse: false }
  }
  if (connected.value) {
    return { dot: '#18a058', text: '实时', pulse: true }
  }
  if (connecting.value) {
    return { dot: '#909399', text: '连接中…', pulse: true }
  }
  return { dot: '#d03050', text: '连接断开 · 5s 自动重连', pulse: true }
})

const statCardStyle = computed(() => ({
  border: `1px solid ${isDark.value ? 'rgba(255,255,255,0.09)' : 'rgba(0,0,0,0.08)'}`,
  background: isDark.value ? '#18181c' : '#ffffff',
  padding: '12px 16px'
}))
</script>

<template>
  <div class="flex flex-col gap-4">
    <!-- 工具栏 -->
    <n-card size="small">
      <div class="flex flex-wrap items-center justify-between gap-3">
        <div class="flex items-center gap-3">
          <span class="text-base font-semibold">接口负载监控</span>
          <span class="text-xs" :style="{ color: isDark ? '#8b8e95' : '#8a8f99' }">
            各节点每秒接收请求数（req/s）· 最后更新 {{ lastFrameTimeText }}
          </span>
        </div>

        <div class="flex items-center gap-3">
          
          <div class="flex items-center gap-2 rounded-full px-3 py-1"
               :style="{ background: isDark ? 'rgba(255,255,255,0.05)' : 'rgba(0,0,0,0.04)' }">
            <span class="status-dot" :class="{ pulse: statusMeta.pulse }"
                  :style="{ background: statusMeta.dot }"></span>
            <span class="text-xs" :style="{ color: isDark ? '#c9cbd1' : '#4a5058' }">
              {{ statusMeta.text }}
            </span>
          </div>
          <n-tooltip>
            <template #trigger>
              <n-button size="small" quaternary @click="togglePause">
                <template #icon>
                  <IconPause v-if="!paused" />
                  <IconPlay v-else />
                </template>
                {{ paused ? '继续' : '暂停' }}
              </n-button>
            </template>
            {{ paused ? '继续接收实时数据' : '暂停刷新（保持连接）' }}
          </n-tooltip>
          <n-tooltip>
            <template #trigger>
              <n-button size="small" quaternary @click="reconnectNow">
                <template #icon><IconRefresh /></template>
                重连
              </n-button>
            </template>
            立即重新建立 SSE 连接
          </n-tooltip>
          <n-radio-group v-model:value="windowMinutes" size="small">
            <n-radio-button value="1">1m</n-radio-button>
            <n-radio-button value="5">5m</n-radio-button>
            <n-radio-button value="15">15m</n-radio-button>
          </n-radio-group>
        </div>
      </div>
    </n-card>

    <!-- 概览统计 -->
    <div class="grid grid-cols-2 gap-3 md:grid-cols-4">
      <div class="rounded-lg px-4 py-3" :style="statCardStyle" >
        <div class="text-xs" :style="{ color: isDark ? '#8b8e95' : '#8a8f99' }">在线节点</div>
        <div class="mono mt-1 text-2xl font-semibold" :style="{ color: isDark ? '#e6e8eb' : '#1f2329' }">
          {{ frameNodeCount }}
        </div>
      </div>
      <div class="rounded-lg px-4 py-3" :style="statCardStyle">
        <div class="text-xs" :style="{ color: isDark ? '#8b8e95' : '#8a8f99' }">当前总速率</div>
        <div class="mono mt-1 text-2xl font-semibold" style="color: #7EB26D;">
          {{ currentTotal }}<span class="ml-1 text-xs font-normal" :style="{ color: isDark ? '#8b8e95' : '#8a8f99' }">req/s</span>
        </div>
      </div>
      <div class="rounded-lg px-4 py-3" :style="statCardStyle">
        <div class="text-xs" :style="{ color: isDark ? '#8b8e95' : '#8a8f99' }">窗口峰值</div>
        <div class="mono mt-1 text-2xl font-semibold" style="color: #EAB839;">
          {{ peakTotal }}<span class="ml-1 text-xs font-normal" :style="{ color: isDark ? '#8b8e95' : '#8a8f99' }">req/s</span>
        </div>
      </div>
      <div class="rounded-lg px-4 py-3" :style="statCardStyle">
        <div class="text-xs" :style="{ color: isDark ? '#8b8e95' : '#8a8f99' }">窗口平均</div>
        <div class="mono mt-1 text-2xl font-semibold" style="color: #6ED0E0;">
          {{ avgTotal }}<span class="ml-1 text-xs font-normal" :style="{ color: isDark ? '#8b8e95' : '#8a8f99' }">req/s</span>
        </div>
      </div>
    </div>

    <!-- 主图 -->
    <n-card size="small">
      <template #header>
        <span class="text-sm font-medium">接口请求速率 / Request Throughput</span>
      </template>
      <template #header-extra>
        <span class="text-xs" :style="{ color: isDark ? '#8b8e95' : '#8a8f99' }">
          每秒推送 · SSE 实时流
        </span>
      </template>
      <div class="chart-wrapper">
        <div ref="chartEl" class="chart-canvas"></div>
        <div v-if="!frameReceived" class="chart-empty">
          <div class="flex flex-col items-center gap-3">
            <span class="chart-spinner"></span>
            <span class="text-sm" :style="{ color: isDark ? '#8b8e95' : '#8a8f99' }">
              等待实时数据…（连接成功后每秒更新）
            </span>
          </div>
        </div>
      </div>
    </n-card>

    <!-- 节点明细（Grafana legend table 风格） -->
    <n-card size="small">
      <template #header>
        <span class="text-sm font-medium">节点明细</span>
      </template>
      <div class="node-table">
        <div class="node-row node-row-head text-xs"
             :style="{ color: isDark ? '#8b8e95' : '#8a8f99' }">
          <span>节点</span>
          <span class="text-right">状态</span>
          <span class="mono text-right">当前</span>
          <span class="mono text-right">平均</span>
          <span class="mono text-right">峰值</span>
        </div>
        <div v-for="row in nodeRows" :key="row.nodeId" class="node-row text-sm"
             :class="{ 'is-offline': row.offline }"
             :style="{ borderTop: `1px solid ${isDark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.06)'}` }">
          <span class="flex items-center gap-2 overflow-hidden">
            <span class="legend-dot" :style="{ background: row.offline ? '#5c5e66' : row.color }"></span>
            <span class="mono truncate">{{ row.nodeId }}</span>
          </span>
          <span class="text-right">
            <n-tag v-if="row.offline" size="small" round :bordered="false" color="#5c5e66" style="color:#fff">
              离线
            </n-tag>
            <n-tag v-else size="small" round :bordered="false"
                   :type="row.state === 'ACTIVE' ? 'success' : 'warning'">
              {{ row.state }}
            </n-tag>
          </span>
          <span class="mono text-right font-semibold">{{ row.current ?? '—' }}</span>
          <span class="mono text-right">{{ row.avg }}</span>
          <span class="mono text-right">{{ row.max }}</span>
        </div>
        <div v-if="nodeRows.length === 0" class="py-8 text-center text-xs"
             :style="{ color: isDark ? '#8b8e95' : '#8a8f99' }">
          暂无节点数据
        </div>
      </div>
    </n-card>
  </div>
</template>

<style scoped>
.chart-wrapper {
  position: relative;
  height: 440px;
}

.chart-canvas {
  position: absolute;
  inset: 0;
  width: 100%;
  height: 100%;
}

.chart-empty {
  position: absolute;
  inset: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  pointer-events: none;
}

.chart-spinner {
  width: 22px;
  height: 22px;
  border-radius: 50%;
  border: 2px solid rgba(126, 178, 109, 0.25);
  border-top-color: #7eb26d;
  animation: chart-spin 0.9s linear infinite;
}

@keyframes chart-spin {
  to {
    transform: rotate(360deg);
  }
}

.status-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  display: inline-block;
}

.status-dot.pulse {
  animation: status-pulse 1.6s ease-in-out infinite;
}

@keyframes status-pulse {
  0%, 100% {
    box-shadow: 0 0 0 0 currentColor;
    opacity: 1;
  }
  50% {
    opacity: 0.55;
  }
}

.node-table {
  display: flex;
  flex-direction: column;
}

.node-row {
  display: grid;
  grid-template-columns: minmax(0, 1.6fr) 90px 80px 80px 80px;
  align-items: center;
  gap: 12px;
  padding: 9px 4px;
}

.node-row-head {
  padding-top: 0;
  padding-bottom: 8px;
}

.legend-dot {
  width: 14px;
  height: 4px;
  border-radius: 2px;
  flex-shrink: 0;
}

.node-row.is-offline {
  opacity: 0.55;
}

@media (max-width: 640px) {
  .node-row {
    grid-template-columns: minmax(0, 1fr) 70px 60px;
  }
  .node-row span:nth-child(4),
  .node-row span:nth-child(5),
  .node-row-head span:nth-child(4),
  .node-row-head span:nth-child(5) {
    display: none;
  }
}
</style>
