<script setup lang="ts">
import { computed, h, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import {
  NButton,
  NCard,
  NDataTable,
  NPopconfirm,
  NProgress,
  NSelect,
  NSpace,
  NStatistic,
  NTag,
  useMessage,
  type DataTableColumns
} from 'naive-ui'
import {
  type CapabilityInfo,
  type MigrationTaskInfo,
  type MigrationTasksResult,
  type PlacementDistribution,
  type RuntimeNodeInfo,
  cancelMigrationTaskApi,
  getCacheNodeStatsApi,
  getMigrationTasksApi,
  getPlacementDistributionApi,
  getStorageNodeStatsApi,
  resumeMigrationApi,
  setNodeDesiredApi,
  triggerRebalanceApi
} from '@/api/cluster'

defineOptions({ name: 'CapabilitySchedulePanel' })

const props = defineProps<{
  /** 调度的能力类型 */
  capability: 'CACHE' | 'STORAGE'
  /** 所在 tab 是否激活（控制轮询启停） */
  active: boolean
  /** 全部能力记录（来自 /rt/status SSE，组件内按 capability 过滤） */
  capabilities: CapabilityInfo[]
  /** 集群节点列表（提供节点存活状态） */
  nodes: RuntimeNodeInfo[]
  activeMigrationTasks: number
  leader: string
}>()

const message = useMessage()

const isStorage = computed(() => props.capability === 'STORAGE')
const countColumnLabel = computed(() => (isStorage.value ? '对象数' : 'Key 条目数'))
const totalCountLabel = computed(() => (isStorage.value ? '集群对象总数' : '集群 Key 总数'))

// ------------------------------------------------------------------
// 数据
// ------------------------------------------------------------------

/** 每节点条目统计（归一化：cache keyCount / storage objectCount）。 */
interface NodeStat {
  nodeId: string
  count: number | null
  reachable: boolean
}

const nodeStats = ref<NodeStat[] | null>(null)
const distribution = ref<PlacementDistribution | null>(null)
const tasksResult = ref<MigrationTasksResult | null>(null)
const taskFilter = ref<{ capability: string; scope: 'active' | 'all' }>({
  capability: props.capability,
  scope: 'active'
})
const rebalanceLoading = ref(false)
const resumeLoading = ref(false)

let taskTimer: ReturnType<typeof setInterval> | null = null
let statsTimer: ReturnType<typeof setInterval> | null = null

const capCapabilities = computed(() =>
  props.capabilities.filter((c) => c.type === props.capability)
)
const capEnabledCount = computed(
  () => capCapabilities.value.filter((c) => c.desired === 'ACTIVE').length
)
const totalCount = computed(() => {
  if (!nodeStats.value) return null
  return nodeStats.value.reduce((sum, s) => sum + (s.count ?? 0), 0)
})

/** 节点能力开关表行：能力记录 × 节点状态 × 每节点统计 join。 */
interface CapNodeRow {
  nodeId: string
  nodeState: string
  desired: string
  actual: string
  count: number | null
  reachable: boolean
}

const capNodeRows = computed<CapNodeRow[]>(() => {
  const stateMap = new Map(props.nodes.map((n) => [n.nodeId, n.state]))
  const statMap = new Map((nodeStats.value || []).map((s) => [s.nodeId, s]))
  return capCapabilities.value.map((c) => {
    const stat = statMap.get(c.nodeId)
    return {
      nodeId: c.nodeId,
      nodeState: stateMap.get(c.nodeId) || 'UNKNOWN',
      desired: c.desired,
      actual: c.actual,
      count: stat?.count ?? null,
      reachable: stat?.reachable ?? false
    }
  })
})

function stateTagType(state: string): 'success' | 'error' | 'warning' | 'default' {
  if (state === 'ACTIVE' || state === 'ALIVE') return 'success'
  if (state === 'DISABLED' || state === 'DEAD') return 'error'
  if (state === 'SUSPECT') return 'warning'
  return 'default'
}

async function handleSetDesired(row: CapabilityInfo, desired: 'ACTIVE' | 'DISABLED') {
  try {
    await setNodeDesiredApi(row.nodeId, row.type, desired)
    message.success(`已将 ${row.nodeId} 的 ${row.type} 能力期望态置为 ${desired}`)
  } catch (e: any) {
    message.error(e?.message || '操作失败')
  }
}

const capNodeColumns = computed<DataTableColumns<CapNodeRow>>(() => [
  { title: '节点 ID', key: 'nodeId', width: 170, ellipsis: { tooltip: true } },
  {
    title: '节点状态', key: 'nodeState', width: 110,
    render: (r) => h(NTag, { type: stateTagType(r.nodeState), size: 'small' }, { default: () => r.nodeState })
  },
  {
    title: '期望态', key: 'desired', width: 100,
    render: (r) => h(NTag, { type: stateTagType(r.desired), size: 'small' }, { default: () => r.desired })
  },
  {
    title: '实际态', key: 'actual', width: 100,
    render: (r) => h(NTag, { type: stateTagType(r.actual), size: 'small' }, { default: () => r.actual })
  },
  {
    title: countColumnLabel.value, key: 'count', width: 130,
    render(r) {
      if (r.actual !== 'ACTIVE') return h('span', { class: 'ww-text-3' }, '-')
      if (!r.reachable) {
        return h(NTag, { type: 'error', size: 'small' }, { default: () => '不可达' })
      }
      return String(r.count ?? 0)
    }
  },
  {
    title: '操作', key: 'actions', width: 120, fixed: 'right',
    render(row) {
      const capInfo: CapabilityInfo = {
        nodeId: row.nodeId,
        type: props.capability,
        desired: row.desired,
        actual: row.actual,
        updatedAtMillis: 0
      }
      if (row.desired === 'ACTIVE') {
        return h(NPopconfirm, { onPositiveClick: () => handleSetDesired(capInfo, 'DISABLED') }, {
          trigger: () => h(NButton, { size: 'small', type: 'warning', quaternary: true }, { default: () => '禁用' }),
          default: () => `禁用 ${row.nodeId} 的 ${props.capability} 能力？调度器将冻结该节点并迁移其分区数据。`
        })
      }
      if (row.desired === 'DISABLED') {
        return h(NPopconfirm, { onPositiveClick: () => handleSetDesired(capInfo, 'ACTIVE') }, {
          trigger: () => h(NButton, { size: 'small', type: 'success', quaternary: true }, { default: () => '启用' }),
          default: () => `启用 ${row.nodeId} 的 ${props.capability} 能力？调度器将拉起能力并重平衡分区。`
        })
      }
      return '-'
    }
  }
])

function primaryPct(row: { primaries: number }): number {
  const total = distribution.value?.partitionCount || 0
  if (total <= 0) return 0
  return Math.min(100, Math.round((row.primaries / total) * 100))
}

// ------------------------------------------------------------------
// 迁移任务
// ------------------------------------------------------------------

const TERMINAL_TASK_STATES = new Set(['COMPLETED', 'FAILED', 'CANCELLED'])
const TASK_STATE_TYPE: Record<string, 'default' | 'info' | 'success' | 'warning' | 'error'> = {
  PENDING: 'default',
  PREPARING: 'info',
  SNAPSHOT: 'info',
  COPYING: 'info',
  SYNCING: 'info',
  CUTOVER: 'warning',
  VERIFYING: 'warning',
  COMPLETED: 'success',
  FAILED: 'error',
  CANCELLED: 'default'
}

const taskCapabilityOptions = [
  { label: 'CACHE', value: 'CACHE' },
  { label: 'STORAGE', value: 'STORAGE' },
  { label: 'MOCK', value: 'MOCK' },
  { label: '全部能力', value: 'ALL' }
]
const taskScopeOptions = [
  { label: '活跃任务', value: 'active' },
  { label: '全部任务', value: 'all' }
]

async function fetchTasks() {
  try {
    tasksResult.value = await getMigrationTasksApi({
      capability: taskFilter.value.capability === 'ALL' ? undefined : taskFilter.value.capability,
      scope: taskFilter.value.scope,
      limit: 100
    })
  } catch {
    // 轮询失败静默（下轮重试）
  }
}

async function fetchNodeStats() {
  try {
    if (isStorage.value) {
      const res = await getStorageNodeStatsApi()
      nodeStats.value = res.nodes.map((s) => ({
        nodeId: s.nodeId,
        count: s.objectCount,
        reachable: s.reachable
      }))
    } else {
      const res = await getCacheNodeStatsApi()
      nodeStats.value = res.nodes.map((s) => ({
        nodeId: s.nodeId,
        count: s.keyCount,
        reachable: s.reachable
      }))
    }
  } catch {
    // ignore
  }
}

async function fetchDistribution() {
  try {
    distribution.value = await getPlacementDistributionApi(props.capability)
  } catch {
    // ignore
  }
}

async function handleCancelTask(task: MigrationTaskInfo) {
  try {
    const res = await cancelMigrationTaskApi(task.taskId)
    if (res.cancelled) {
      message.success(`任务 #${task.taskId} 已取消`)
    } else {
      message.warning(res.message || `任务 #${task.taskId} 无法取消`)
    }
  } catch (e: any) {
    message.error(e?.message || '取消失败')
  } finally {
    fetchTasks()
  }
}

async function handleResumeMigration() {
  resumeLoading.value = true
  try {
    await resumeMigrationApi()
    message.success('已触发失败/中断迁移的断点续跑')
  } catch (e: any) {
    message.error(e?.message || '操作失败')
  } finally {
    resumeLoading.value = false
    fetchTasks()
  }
}

async function handleTriggerRebalance() {
  rebalanceLoading.value = true
  try {
    const res = await triggerRebalanceApi(props.capability)
    if (res.isLeader) {
      message.success(`已重置 ${props.capability} 重平衡冷却，Leader 将在下个调度周期重新规划（受最小移动比例门控）`)
    } else {
      message.warning(
        `当前连接节点非 Leader（Leader: ${res.leader || '未知'}），本次触发未生效——请经 Leader 节点入口重试`
      )
    }
  } catch (e: any) {
    message.error(e?.message || '操作失败')
  } finally {
    rebalanceLoading.value = false
    fetchTasks()
  }
}

const taskColumns: DataTableColumns<MigrationTaskInfo> = [
  { title: '任务 ID', key: 'taskId', width: 90 },
  {
    title: '能力', key: 'capability', width: 100,
    render: (r) => h(NTag, { size: 'small' }, { default: () => r.capability })
  },
  { title: '分区', key: 'partition', width: 90 },
  {
    title: '源 → 目标', key: 'route', width: 200, ellipsis: { tooltip: true },
    render: (r) => `${r.fromNode || '-'} → ${r.toNode}`
  },
  {
    title: '状态', key: 'state', width: 110,
    render: (r) => h(
      NTag,
      { type: TASK_STATE_TYPE[r.state] || 'default', size: 'small' },
      { default: () => r.state }
    )
  },
  {
    title: '进度', key: 'progress', width: 200,
    render(r) {
      if (r.sourceCount > 0) {
        const pct = Math.min(
          100,
          Math.round((r.itemsDone / Math.max(r.sourceCount, r.itemsDone, 1)) * 100)
        )
        return h('div', { style: 'display:flex;align-items:center;gap:8px;min-width:150px' }, [
          h(NProgress, {
            type: 'line',
            percentage: pct,
            showIndicator: false,
            style: 'flex:1'
          }),
          h('span', { class: 'text-xs ww-text-3 whitespace-nowrap' },
            `${r.itemsDone}/${r.sourceCount}`)
        ])
      }
      return h('span', { class: 'text-xs ww-text-3' }, `${r.itemsDone} 条`)
    }
  },
  {
    title: '更新时间', key: 'updatedAtMillis', width: 170,
    render: (r) => (r.updatedAtMillis ? new Date(r.updatedAtMillis).toLocaleString() : '-')
  },
  {
    title: '操作', key: 'actions', width: 100, fixed: 'right',
    render(row) {
      const disabled = TERMINAL_TASK_STATES.has(row.state)
      return h(
        NButton,
        {
          size: 'small',
          type: 'error',
          quaternary: true,
          disabled,
          onClick: () => handleCancelTask(row)
        },
        { default: () => '取消' }
      )
    }
  }
]

// ------------------------------------------------------------------
// 轮询（tab 激活 + 页面可见时）
// ------------------------------------------------------------------

function stopPolling() {
  if (taskTimer) {
    clearInterval(taskTimer)
    taskTimer = null
  }
  if (statsTimer) {
    clearInterval(statsTimer)
    statsTimer = null
  }
}

function restartPolling() {
  stopPolling()
  if (!props.active || document.visibilityState !== 'visible') {
    return
  }
  fetchTasks()
  fetchNodeStats()
  fetchDistribution()
  taskTimer = setInterval(fetchTasks, taskFilter.value.scope === 'all' ? 5000 : 2000)
  statsTimer = setInterval(() => {
    fetchNodeStats()
    fetchDistribution()
  }, 10000)
}

function handleVisibilityChange() {
  restartPolling()
}

watch(() => props.active, restartPolling)
watch(() => taskFilter.value.scope, restartPolling)
watch(() => taskFilter.value.capability, fetchTasks)

onMounted(() => {
  document.addEventListener('visibilitychange', handleVisibilityChange)
  restartPolling()
})

onBeforeUnmount(() => {
  document.removeEventListener('visibilitychange', handleVisibilityChange)
  stopPolling()
})
</script>

<template>
  <div>
    <div class="mb-3 grid grid-cols-2 gap-3 lg:grid-cols-4">
      <NCard size="small">
        <NStatistic :label="`${capability} 开启节点`">
          <template #default>
            <span :class="capEnabledCount > 0 ? 'text-green-600 dark:text-green-400' : 'ww-text-3'">
              {{ capEnabledCount }} / {{ capCapabilities.length }}
            </span>
          </template>
        </NStatistic>
      </NCard>
      <NCard size="small">
        <NStatistic :label="totalCountLabel" :value="totalCount === null ? '-' : totalCount" />
      </NCard>
      <NCard size="small">
        <NStatistic label="活跃迁移任务" :value="activeMigrationTasks">
          <template #suffix>
            <NTag v-if="activeMigrationTasks > 0" type="warning" size="small">迁移中</NTag>
          </template>
        </NStatistic>
      </NCard>
      <NCard size="small">
        <NStatistic label="当前 Leader">
          <template #default>
            <span class="mono text-base">{{ leader }}</span>
          </template>
        </NStatistic>
      </NCard>
    </div>

    <NCard :title="`节点 ${capability} 开关`" size="small" class="mb-3">
      <NDataTable
        :columns="capNodeColumns"
        :data="capNodeRows"
        :bordered="false"
        :single-line="false"
        size="small"
        :scroll-x="820"
      />
    </NCard>

    <NCard :title="`${capability} 分区分布`" size="small" class="mb-3">
      <template #header-extra>
        <span v-if="distribution" class="text-xs ww-text-3">
          共 {{ distribution.partitionCount }} 个分区
        </span>
      </template>
      <div v-if="distribution && distribution.distribution.length">
        <div v-for="row in distribution.distribution" :key="row.nodeId" class="mb-2 last:mb-0">
          <div class="mb-1 flex items-center justify-between text-xs">
            <span class="font-medium">{{ row.nodeId }}</span>
            <span class="ww-text-3">
              主 {{ row.primaries }}
              <template v-if="row.replicas > 0"> · 副本 {{ row.replicas }}</template>
              / {{ distribution.partitionCount }}
            </span>
          </div>
          <div class="h-2 overflow-hidden rounded bg-[var(--ww-surface-hover)]">
            <div
              class="h-full rounded bg-green-500 dark:bg-green-400 transition-all"
              :style="{ width: primaryPct(row) + '%' }"
            />
          </div>
        </div>
      </div>
      <div v-else class="text-xs ww-text-3">
        放置表尚未引导（无 {{ capability }} 开启节点）或加载中…
      </div>
    </NCard>

    <NCard title="调度操作" size="small" class="mb-3">
      <NSpace :size="12">
        <NPopconfirm @positive-click="handleTriggerRebalance">
          <template #trigger>
            <NButton type="primary" size="small" :loading="rebalanceLoading">
              手动触发重平衡
            </NButton>
          </template>
          重置 {{ capability }} 重平衡冷却，Leader 将在下个调度周期重新规划分区分布
          （受最小移动比例门控；仅对 Leader 节点入口生效）。
        </NPopconfirm>
        <NPopconfirm @positive-click="handleResumeMigration">
          <template #trigger>
            <NButton type="warning" size="small" :loading="resumeLoading">
              重试失败迁移
            </NButton>
          </template>
          将 FAILED 迁移任务重置为 PENDING 并按断点续跑（幂等）。
        </NPopconfirm>
      </NSpace>
    </NCard>

    <NCard title="迁移任务" size="small">
      <template #header-extra>
        <NSpace :size="8" align="center">
          <NTag v-if="tasksResult?.truncated" type="warning" size="small">
            已截断，共 {{ tasksResult.total }} 条
          </NTag>
          <span v-else-if="tasksResult" class="text-xs ww-text-3">
            共 {{ tasksResult.total }} 条
          </span>
          <NSelect
            v-model:value="taskFilter.capability"
            :options="taskCapabilityOptions"
            size="small"
            style="width: 130px"
          />
          <NSelect
            v-model:value="taskFilter.scope"
            :options="taskScopeOptions"
            size="small"
            style="width: 120px"
          />
          <NButton size="tiny" quaternary @click="fetchTasks">刷新</NButton>
        </NSpace>
      </template>
      <NDataTable
        :columns="taskColumns"
        :data="tasksResult?.tasks || []"
        :bordered="false"
        :single-line="false"
        size="small"
        :scroll-x="1100"
        :pagination="taskFilter.scope === 'all' ? { pageSize: 20 } : false"
      />
    </NCard>
  </div>
</template>
