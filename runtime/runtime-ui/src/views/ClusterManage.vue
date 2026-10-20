<script setup lang="ts">
import { computed, h, onBeforeUnmount, onMounted, ref } from 'vue'
import {
  NAlert,
  NButton,
  NCard,
  NCheckbox,
  NDataTable,
  NDynamicTags,
  NForm,
  NFormItem,
  NInput,
  NInputNumber,
  NModal,
  NPopconfirm,
  NProgress,
  NRadio,
  NRadioGroup,
  NSelect,
  NSpace,
  NStatistic,
  NSwitch,
  NTabPane,
  NTabs,
  NTag,
  useMessage,
  type DataTableColumns
} from 'naive-ui'
import CapabilitySchedulePanel from '@/components/CapabilitySchedulePanel.vue'
import {
  type CapabilityInfo,
  type ClusterStatusStreamHandle,
  type DecommissionPrecheck,
  type DecommissionStatus,
  type RuntimeNodeInfo,
  type RuntimeStatus,
  decommissionNodeApi,
  decommissionPrecheckApi,
  getClusterNodesApi,
  getDecommissionStatusApi,
  getRuntimeStatus,
  placementLookupApi,
  setNodeDesiredApi,
  startClusterStatusStreamApi
} from '@/api/cluster'
import { getLbConfigApi, updateLbConfigApi, type LbConfig, type LbStrategyName } from '@/api/lb'

defineOptions({ name: 'ClusterManage' })

const message = useMessage()

const runtimeStatus = ref<RuntimeStatus | null>(null)
const nodes = ref<RuntimeNodeInfo[]>([])
const streamConnected = ref(false)
const lastSnapshotTime = ref(0)
let streamHandle: ClusterStatusStreamHandle | null = null

const selfNodeId = computed(
  () => runtimeStatus.value?.self?.nodeId || runtimeStatus.value?.nodeId || ''
)
const isLeader = computed(() => runtimeStatus.value?.isLeader ?? false)
const leader = computed(() => runtimeStatus.value?.leader || '-')
const leaseOwner = computed(() => runtimeStatus.value?.leaseOwner || '-')
const activeMigrationTasks = computed(() => runtimeStatus.value?.activeMigrationTasks || 0)
const decommissions = computed(() => runtimeStatus.value?.decommissions || [])

const memberCount = computed(() => runtimeStatus.value?.members?.length || 0)
const aliveCount = computed(() =>
  runtimeStatus.value?.members?.filter((m) => m.state === 'ACTIVE' || m.state === 'ALIVE').length || 0
)

/** 能力汇总：按类型聚合期望/实际 ACTIVE 数（吸收自集群概览） */
const capabilityStats = computed(() => {
  const stats: Record<string, { active: number; desired: number }> = {}
  for (const cap of runtimeStatus.value?.capabilities || []) {
    if (!stats[cap.type]) {
      stats[cap.type] = { active: 0, desired: 0 }
    }
    if (cap.actual === 'ACTIVE') stats[cap.type].active++
    if (cap.desired === 'ACTIVE') stats[cap.type].desired++
  }
  return Object.entries(stats).map(([type, s]) => ({ type, ...s }))
})

function isDecommissioning(nodeId: string): boolean {
  return decommissions.value.some((d) => d.nodeId === nodeId)
}

function decommissionState(nodeId: string): string | null {
  return decommissions.value.find((d) => d.nodeId === nodeId)?.state || null
}

// ------------------------------------------------------------------
// 退役弹窗
// ------------------------------------------------------------------

const decommissionModalVisible = ref(false)
const decommissionStep = ref<'precheck' | 'progress'>('precheck')
const decommissionTargetNode = ref<string>('')
const precheckLoading = ref(false)
const precheckResult = ref<DecommissionPrecheck | null>(null)
const decommissionStatus = ref<DecommissionStatus | null>(null)
const forceConfirm = ref(false)
let progressTimer: ReturnType<typeof setInterval> | null = null

async function handleDecommissionClick(row: RuntimeNodeInfo) {
  decommissionTargetNode.value = row.nodeId
  decommissionStep.value = 'precheck'
  precheckResult.value = null
  decommissionStatus.value = null
  forceConfirm.value = false
  decommissionModalVisible.value = true
  precheckLoading.value = true
  try {
    precheckResult.value = await decommissionPrecheckApi(row.nodeId)
  } catch (e: any) {
    message.error(e?.message || '预检失败')
    decommissionModalVisible.value = false
  } finally {
    precheckLoading.value = false
  }
}

async function handleDecommissionConfirm() {
  try {
    await decommissionNodeApi(decommissionTargetNode.value, forceConfirm.value)
    message.success(`已启动节点 ${decommissionTargetNode.value} 的退役流程`)
    decommissionStep.value = 'progress'
    startProgressPolling()
  } catch (e: any) {
    message.error(e?.message || '启动退役失败')
  }
}

function startProgressPolling() {
  stopProgressPolling()
  pollProgress()
  progressTimer = setInterval(pollProgress, 2000)
}

function stopProgressPolling() {
  if (progressTimer) {
    clearInterval(progressTimer)
    progressTimer = null
  }
}

async function pollProgress() {
  try {
    const status = await getDecommissionStatusApi(decommissionTargetNode.value)
    decommissionStatus.value = status
    if (status.record?.state === 'COMPLETED' || status.record?.state === 'FAILED') {
      stopProgressPolling()
    }
  } catch {
    // ignore
  }
}

function handleDecommissionModalClose() {
  stopProgressPolling()
  decommissionModalVisible.value = false
}

const capabilities = computed<CapabilityInfo[]>(
  () => runtimeStatus.value?.capabilities || []
)

function applySnapshot(status: RuntimeStatus) {
  lastSnapshotTime.value = Date.now()
  runtimeStatus.value = status
  if (Array.isArray(status.members) && status.members.length) {
    nodes.value = status.members
  }
}

async function fetchNodes() {
  try {
    const list = await getClusterNodesApi()
    if (list && list.length) {
      nodes.value = list
    } else if (runtimeStatus.value?.members?.length) {
      nodes.value = runtimeStatus.value.members
    }
  } catch {
    // ignore
  }
}

function startStream() {
  stopStream()
  streamHandle = startClusterStatusStreamApi(applySnapshot, (connected) => {
    streamConnected.value = connected
  })
}

function stopStream() {
  if (streamHandle) {
    streamHandle.stop()
    streamHandle = null
  }
  streamConnected.value = false
}

async function handleSetDesired(row: CapabilityInfo, desired: 'ACTIVE' | 'DISABLED') {
  try {
    await setNodeDesiredApi(row.nodeId, row.type, desired)
    message.success(`已将 ${row.nodeId} 的 ${row.type} 能力期望态置为 ${desired}`)
  } catch (e: any) {
    message.error(e?.message || '操作失败')
  }
}

const nodeColumns: DataTableColumns<RuntimeNodeInfo> = [
  {
    title: '节点 ID', key: 'nodeId', width: 200,
    render(row) {
      const children: Array<string | ReturnType<typeof h>> = [row.nodeId]
      if (row.nodeId === selfNodeId.value) {
        children.push(h(NTag, { size: 'small', type: 'info' }, { default: () => '本机' }))
      }
      return h(NSpace, { size: 6, align: 'center', wrap: false }, { default: () => children })
    }
  },
  { title: 'Host', key: 'host', width: 140 },
  { title: 'gRPC 端口', key: 'grpcPort', width: 100 },
  {
    title: '状态', key: 'state', width: 100,
    render(row) {
      const typeMap: Record<string, 'error' | 'success' | 'warning' | 'info' | 'default'> = {
        ACTIVE: 'success', JOINING: 'info', SUSPECTED: 'warning',
        LEAVING: 'warning', DEAD: 'error'
      }
      return h(
        NTag,
        { type: typeMap[row.state] || 'default', size: 'small' },
        { default: () => row.state || 'UNKNOWN' }
      )
    }
  },
  { title: 'State Epoch', key: 'stateEpoch', width: 120 },
  {
    // Phi Accrual 本地怀疑度（来自 /rt/status members；/rt/nodes 列表无此值，显示 -）：
    // φ≥3 疑似（黄）、φ≥8 死亡候选（红）
    title: 'φ 怀疑度', key: 'phi', width: 100,
    render(row) {
      const phi = row.phi
      if (typeof phi !== 'number') return '-'
      const type: 'error' | 'warning' | 'success' =
        phi >= 8 ? 'error' : phi >= 3 ? 'warning' : 'success'
      return h(NTag, { type, size: 'small' }, { default: () => phi.toFixed(2) })
    }
  },
  {
    title: '权重', key: 'weight', width: 80,
    render: (r) => (r.weight ? r.weight.toFixed(1) : '-')
  },
  {
    title: '操作', key: 'actions', width: 120, fixed: 'right',
    render(row) {
      if (row.state === 'DEAD' || row.state === 'LEAVING') return null
      if (isDecommissioning(row.nodeId)) {
        const state = decommissionState(row.nodeId)
        return h(NSpace, { size: 4, align: 'center' }, {
          default: () => [
            h(NButton, { size: 'small', disabled: true, type: 'error', quaternary: true },
              { default: () => '退役中' }),
            h(NTag, { size: 'small', type: 'warning' }, { default: () => state || 'DRAINING' })
          ]
        })
      }
      return h(NButton, {
        size: 'small', type: 'error', quaternary: true,
        onClick: () => handleDecommissionClick(row)
      }, { default: () => '退役' })
    }
  }
]

// ------------------------------------------------------------------
// 能力列表（类型筛选）
// ------------------------------------------------------------------

const capabilityTypeFilter = ref<string>('ALL')
const capabilityTypeOptions = [
  { label: '全部类型', value: 'ALL' },
  { label: 'CACHE', value: 'CACHE' },
  { label: 'STORAGE', value: 'STORAGE' },
  { label: 'MOCK', value: 'MOCK' }
]
const filteredCapabilities = computed(() =>
  capabilityTypeFilter.value === 'ALL'
    ? capabilities.value
    : capabilities.value.filter((c) => c.type === capabilityTypeFilter.value)
)

const capabilityColumns: DataTableColumns<CapabilityInfo> = [
  { title: '节点 ID', key: 'nodeId', width: 170, ellipsis: { tooltip: true } },
  {
    title: '类型', key: 'type', width: 100,
    render: (r) => h(NTag, { size: 'small' }, { default: () => r.type })
  },
  {
    title: '期望态', key: 'desired', width: 100,
    render(row) {
      const type =
        row.desired === 'ACTIVE' ? 'success' :
        row.desired === 'DISABLED' ? 'error' : 'default'
      return h(NTag, { type, size: 'small' }, { default: () => row.desired })
    }
  },
  {
    title: '实际态', key: 'actual', width: 100,
    render(row) {
      const type =
        row.actual === 'ACTIVE' ? 'success' :
        row.actual === 'DISABLED' ? 'error' : 'default'
      return h(NTag, { type, size: 'small' }, { default: () => row.actual })
    }
  },
  {
    title: '更新时间', key: 'updatedAtMillis', width: 180,
    render: (r) => (r.updatedAtMillis ? new Date(r.updatedAtMillis).toLocaleString() : '-')
  },
  {
    title: '操作', key: 'actions', width: 140, fixed: 'right',
    render(row) {
      if (row.desired === 'ACTIVE') {
        return h(NPopconfirm, { onPositiveClick: () => handleSetDesired(row, 'DISABLED') }, {
          trigger: () => h(NButton, { size: 'small', type: 'warning', quaternary: true }, { default: () => '禁用' }),
          default: () => `确定将 ${row.nodeId} 的 ${row.type} 能力置为 DISABLED？`
        })
      }
      if (row.desired === 'DISABLED') {
        return h(NPopconfirm, { onPositiveClick: () => handleSetDesired(row, 'ACTIVE') }, {
          trigger: () => h(NButton, { size: 'small', type: 'success', quaternary: true }, { default: () => '启用' }),
          default: () => `确定将 ${row.nodeId} 的 ${row.type} 能力置为 ACTIVE？`
        })
      }
      return '-'
    }
  }
]

// ------------------------------------------------------------------
// 调度面板（缓存 / 存储复用 CapabilitySchedulePanel）
// ------------------------------------------------------------------

const activeTab = ref('nodes')

// ------------------------------------------------------------------
// 分区路由查询
// ------------------------------------------------------------------

type PlacementResult = Awaited<ReturnType<typeof placementLookupApi>>
const placementForm = ref({ capability: 'CACHE', key: '' })
const placementResult = ref<PlacementResult | null>(null)
const placementLoading = ref(false)

const capabilityOptions = [
  { label: 'CACHE', value: 'CACHE' },
  { label: 'STORAGE', value: 'STORAGE' },
  { label: 'MOCK', value: 'MOCK' }
]

async function handlePlacementLookup() {
  if (!placementForm.value.key) {
    message.warning('请输入 Key')
    return
  }
  placementLoading.value = true
  placementResult.value = null
  try {
    placementResult.value = await placementLookupApi(
      placementForm.value.capability,
      placementForm.value.key
    )
  } catch (e: any) {
    message.error(e?.message || '查询失败')
  } finally {
    placementLoading.value = false
  }
}

// ------------------------------------------------------------------
// 负载均衡配置（无上游 LBS/Nginx 时启用；/rt/lb/config）
// ------------------------------------------------------------------

const lbForm = ref<LbConfig | null>(null)
const lbSaving = ref(false)

const lbStrategyOptions: Array<{ label: string; value: LbStrategyName }> = [
  { label: '轮询', value: 'ROUND_ROBIN' },
  { label: '平滑权重', value: 'WEIGHTED' },
  { label: '随机', value: 'RANDOM' }
]

/** 入口下拉：仅 ACTIVE 且已上报 HTTP 端口的节点，显示 nodeId(host:httpPort)。 */
const lbEntryOptions = computed(() =>
  nodes.value
    .filter((n) => n.state === 'ACTIVE')
    .map((n) => ({
      label: `${n.nodeId}（${n.host}:${(n as any).httpPort || '?'}）`,
      value: n.nodeId
    }))
)

/** 权重覆盖表：ACTIVE 节点的注册权重 + 可填覆盖值。 */
const lbWeightRows = computed(() =>
  nodes.value
    .filter((n) => n.state === 'ACTIVE')
    .map((n) => ({
      nodeId: n.nodeId,
      host: n.host,
      httpPort: (n as any).httpPort || 0,
      registeredWeight: n.weight,
      overrideWeight: lbForm.value?.weightOverrides?.[n.nodeId] ?? null
    }))
)

const lbEntryAddress = computed(() => {
  const entry = nodes.value.find((n) => n.nodeId === lbForm.value?.entryNodeId)
  if (!entry) return ''
  return `${entry.host}:${(entry as any).httpPort || '?'}`
})

/** 当前策略的中文标签 */
const lbStrategyLabel = computed(
  () => lbStrategyOptions.find((s) => s.value === lbForm.value?.strategy)?.label || ''
)

/** 权重覆盖表列定义：覆盖输入留空（null）即回退节点注册权重。 */
const lbWeightColumns: DataTableColumns<{
  nodeId: string
  host: string
  httpPort: number
  registeredWeight: number
  overrideWeight: number | null
}> = [
  { title: '节点', key: 'nodeId', width: 110 },
  {
    title: 'HTTP 地址', key: 'addr', width: 190,
    render: (row) => `${row.host}:${row.httpPort || '?'}`
  },
  { title: '注册权重', key: 'registeredWeight', width: 100 },
  {
    title: 'LB 权重覆盖', key: 'overrideWeight', width: 150,
    render: (row) =>
      h(NInputNumber, {
        value: row.overrideWeight,
        size: 'small',
        min: 0.01,
        step: 0.5,
        clearable: true,
        placeholder: '默认',
        style: 'width: 130px',
        'onUpdate:value': (v: number | null) => setWeightOverride(row.nodeId, v)
      })
  }
]

async function loadLbConfig() {
  try {
    lbForm.value = await getLbConfigApi()
  } catch (e: any) {
    message.error(e?.message || '负载均衡配置加载失败')
  }
}

function setWeightOverride(nodeId: string, value: number | null) {
  if (!lbForm.value) return
  const overrides = { ...lbForm.value.weightOverrides }
  if (value === null || value === undefined || Number.isNaN(value) || value <= 0) {
    delete overrides[nodeId]
  } else {
    overrides[nodeId] = value
  }
  lbForm.value = { ...lbForm.value, weightOverrides: overrides }
}

/** 与后端 LbConfig.isProtectedPrefix 同口径的前端拦截。 */
function isProtectedPrefix(prefix: string): boolean {
  const p = prefix.trim().toLowerCase()
  if (p === '/' || p === '/index.html' || p === '/favicon.ico') return true
  return ['/rt', '/api', '/assets'].some((root) => p === root || p.startsWith(root + '/'))
}

async function saveLbConfig() {
  if (!lbForm.value) return
  const cfg = lbForm.value
  if (cfg.enabled && !cfg.entryNodeId) {
    message.warning('启用负载均衡时必须指定入口节点')
    return
  }
  if (cfg.enabled && cfg.pathPrefixes.length === 0) {
    message.warning('至少配置一个代理路径前缀')
    return
  }
  for (const prefix of cfg.pathPrefixes) {
    if (!prefix.startsWith('/')) {
      message.warning(`路径前缀必须以 / 开头：${prefix}`)
      return
    }
    if (isProtectedPrefix(prefix)) {
      message.warning(`控制面/静态资源前缀不允许代理：${prefix}`)
      return
    }
  }
  lbSaving.value = true
  try {
    lbForm.value = await updateLbConfigApi(cfg)
    message.success('负载均衡配置已保存（入口节点 3s 内生效）')
  } catch (e: any) {
    const msg: string = e?.message || '保存失败'
    if (msg.includes('conflict')) {
      message.error('配置已被其他节点修改，请稍后重试（已自动刷新最新版本）')
      await loadLbConfig()
    } else {
      message.error(msg)
    }
  } finally {
    lbSaving.value = false
  }
}

onMounted(async () => {
  try {
    const status = await getRuntimeStatus()
    applySnapshot(status)
  } catch {
    // ignore
  }
  if (!nodes.value.length) {
    await fetchNodes()
  }
  await loadLbConfig()
  startStream()
})

onBeforeUnmount(() => {
  stopStream()
  stopProgressPolling()
})
</script>

<template>
  <div class="h-full overflow-auto">
    <div class="mb-3 flex items-center justify-between">
      <NSpace :size="8" align="center">
        <NTag :type="streamConnected ? 'success' : 'warning'" size="small" round>
          {{ streamConnected ? '实时推送中' : '连接断开，重连中…' }}
        </NTag>
        <span v-if="lastSnapshotTime" class="text-xs ww-text-3">
          最后更新: {{ new Date(lastSnapshotTime).toLocaleTimeString() }}
        </span>
        <NTag v-if="selfNodeId" size="small">本节点: {{ selfNodeId }}</NTag>
      </NSpace>
      <NButton size="tiny" quaternary @click="startStream">重建连接</NButton>
    </div>

    <div class="mb-3 grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-6">
      <NCard size="small">
        <NStatistic label="集群节点" :value="memberCount">
          <template #suffix>
            <span class="text-xs ww-text-3">个</span>
          </template>
        </NStatistic>
      </NCard>
      <NCard size="small">
        <NStatistic label="存活节点" :value="aliveCount">
          <template #suffix>
            <span
              class="text-xs"
              :class="memberCount === 0 ? 'ww-text-3' : aliveCount === memberCount ? 'text-green-500 dark:text-green-400' : 'text-orange-500'"
            >
              {{ memberCount === 0 ? '无节点' : aliveCount === memberCount ? '全部存活' : '有离线' }}
            </span>
          </template>
        </NStatistic>
      </NCard>
      <NCard size="small">
        <NStatistic label="当前角色" :value="isLeader ? 'Leader' : 'Follower'">
          <template #suffix>
            <NTag :type="isLeader ? 'success' : 'default'" size="small">
              {{ isLeader ? '主节点' : '从节点' }}
            </NTag>
          </template>
        </NStatistic>
      </NCard>
      <NCard size="small">
        <NStatistic label="Leader 节点">
          <template #default>
            <span class="mono text-base">{{ leader }}</span>
          </template>
        </NStatistic>
      </NCard>
      <NCard size="small">
        <NStatistic label="Lease 持有者">
          <template #default>
            <span class="mono text-base">{{ leaseOwner }}</span>
          </template>
        </NStatistic>
      </NCard>
      <NCard size="small">
        <NStatistic label="活跃迁移" :value="activeMigrationTasks">
          <template #suffix>
            <NTag v-if="activeMigrationTasks > 0" type="warning" size="small">
              迁移中
            </NTag>
            <span v-else class="text-xs ww-text-3">个任务</span>
          </template>
        </NStatistic>
      </NCard>
    </div>

    <NTabs v-model:value="activeTab" type="line" animated>
      <NTabPane name="nodes" tab="节点列表">
        <NCard title="节点列表" size="small">
          <template #header-extra>
            <NButton size="tiny" quaternary @click="fetchNodes">刷新</NButton>
          </template>
          <NDataTable
            :columns="nodeColumns"
            :data="nodes"
            :bordered="false"
            :single-line="false"
            size="small"
            :scroll-x="840"
          />
        </NCard>
      </NTabPane>

      <NTabPane name="lb" tab="负载均衡">
        <NCard title="负载均衡（无上游 LBS/Nginx 时启用）" size="small">
          <template #header-extra>
            <NButton size="tiny" quaternary @click="loadLbConfig">刷新</NButton>
          </template>

          <NAlert v-if="lbForm?.enabled" type="info" class="mb-3" :show-icon="true">
            已启用：客户端海量请求必须指向入口节点
            <b>{{ lbForm.entryNodeId }}（{{ lbEntryAddress }}）</b>
            ，由入口按「{{ lbStrategyLabel }}」分发；
            上游若存在 LBS/Nginx，请关闭此开关。
          </NAlert>
          <NAlert v-else type="default" class="mb-3" :show-icon="true">
            已关闭：当前形态假定上游存在 LBS/Nginx，请求直达各节点，过滤器直通不干预。
          </NAlert>

          <NForm v-if="lbForm" label-placement="left" label-width="160px">
            <NFormItem label="启用负载均衡">
              <NSwitch v-model:value="lbForm.enabled" />
              <span class="ml-2 text-xs ww-text-3">关闭后所有节点直接对外服务</span>
            </NFormItem>

            <NFormItem label="流量入口节点">
              <NSelect
                v-model:value="lbForm.entryNodeId"
                :options="lbEntryOptions"
                :disabled="!lbForm.enabled"
                clearable
                placeholder="选择唯一流量入口"
                style="width: 320px"
              />
            </NFormItem>

            <NFormItem label="分发策略">
              <NRadioGroup v-model:value="lbForm.strategy">
                <NRadio v-for="opt in lbStrategyOptions" :key="opt.value" :value="opt.value">
                  {{ opt.label }}
                </NRadio>
              </NRadioGroup>
            </NFormItem>

            <NFormItem label="入口本机参与处理">
              <NSwitch v-model:value="lbForm.selfServe" />
              <span class="ml-2 text-xs ww-text-3">关闭后入口只做转发，不处理请求</span>
            </NFormItem>

            <NFormItem label="代理路径前缀">
              <NDynamicTags v-model:value="lbForm.pathPrefixes" :max="20" />
            </NFormItem>
            <div class="mb-3 -mt-2 pl-40 text-xs ww-text-3">
              PathPattern 语法，默认 /cache/**、/storage/**；/rt、/api、/assets 与根路径受保护不可配置
            </div>

            <NFormItem label="节点权重覆盖">
              <NDataTable
                :columns="lbWeightColumns"
                :data="lbWeightRows"
                :bordered="false"
                size="small"
                :scroll-x="560"
                style="width: 560px"
              />
            </NFormItem>
            <div class="mb-3 -mt-2 pl-40 text-xs ww-text-3">
              覆盖值留空=使用节点注册权重；覆盖仅影响负载均衡，不影响分区放置
            </div>

            <NFormItem label=" ">
              <NSpace>
                <NButton type="primary" :loading="lbSaving" @click="saveLbConfig">保存配置</NButton>
                <NButton quaternary @click="loadLbConfig">重新加载</NButton>
                <span class="text-xs ww-text-3">配置版本 version={{ lbForm.version }}</span>
              </NSpace>
            </NFormItem>
          </NForm>
        </NCard>
      </NTabPane>

      <NTabPane name="capabilities" tab="能力列表">
        <NCard title="能力列表" size="small">
          <template #header-extra>
            <NSelect
              v-model:value="capabilityTypeFilter"
              :options="capabilityTypeOptions"
              size="small"
              style="width: 140px"
            />
          </template>
          <NSpace v-if="capabilityStats.length" :size="8" class="mb-3">
            <NTag
              v-for="s in capabilityStats"
              :key="s.type"
              :type="s.active === s.desired ? 'success' : 'warning'"
              size="small"
              round
            >
              {{ s.type }}：实际 {{ s.active }} / 期望 {{ s.desired }} ACTIVE
            </NTag>
          </NSpace>
          <NDataTable
            :columns="capabilityColumns"
            :data="filteredCapabilities"
            :bordered="false"
            :single-line="false"
            size="small"
            :scroll-x="820"
          />
        </NCard>
      </NTabPane>

      <NTabPane name="cache" tab="缓存调度">
        <CapabilitySchedulePanel
          capability="CACHE"
          :active="activeTab === 'cache'"
          :capabilities="capabilities"
          :nodes="nodes"
          :active-migration-tasks="activeMigrationTasks"
          :leader="leader"
        />
      </NTabPane>

      <NTabPane name="storage" tab="存储调度">
        <CapabilitySchedulePanel
          capability="STORAGE"
          :active="activeTab === 'storage'"
          :capabilities="capabilities"
          :nodes="nodes"
          :active-migration-tasks="activeMigrationTasks"
          :leader="leader"
        />
      </NTabPane>

      <NTabPane name="placement" tab="分区路由查询">
        <NCard title="分区路由查询" size="small">
          <NForm label-placement="left" label-width="110px" inline>
            <NFormItem label="Capability">
              <NSelect
                v-model:value="placementForm.capability"
                :options="capabilityOptions"
                style="width: 160px"
              />
            </NFormItem>
            <NFormItem label="Key">
              <NInput
                v-model:value="placementForm.key"
                placeholder="输入 key"
                style="width: 280px"
                @keyup.enter="handlePlacementLookup"
              />
            </NFormItem>
            <NFormItem label=" ">
              <NButton
                type="primary"
                :loading="placementLoading"
                @click="handlePlacementLookup"
              >
                查询
              </NButton>
            </NFormItem>
          </NForm>
          <div v-if="placementResult" class="mt-3">
            <NDescriptions label-placement="left" :column="3" size="small" bordered>
              <NDescriptionsItem label="Capability">
                {{ placementResult.capability }}
              </NDescriptionsItem>
              <NDescriptionsItem label="Key">
                {{ placementResult.key }}
              </NDescriptionsItem>
              <NDescriptionsItem label="Partition">
                {{ placementResult.partition }}
              </NDescriptionsItem>
              <NDescriptionsItem label="Owner Node">
                {{ placementResult.ownerNode }}
              </NDescriptionsItem>
              <NDescriptionsItem label="Epoch">
                {{ placementResult.epoch }}
              </NDescriptionsItem>
              <NDescriptionsItem label="Host">
                {{ placementResult.host }}
              </NDescriptionsItem>
              <NDescriptionsItem label="gRPC Port">
                {{ placementResult.grpcPort }}
              </NDescriptionsItem>
              <NDescriptionsItem label="查询节点 HTTP 端口">
                {{ placementResult.queriedNodeHttpPort }}
              </NDescriptionsItem>
            </NDescriptions>
          </div>
        </NCard>
      </NTabPane>
    </NTabs>

    <NModal
      v-model:show="decommissionModalVisible"
      preset="card"
      title="节点退役"
      style="width: 660px"
      :mask-closable="false"
      @after-leave="handleDecommissionModalClose"
    >
      <div v-if="decommissionStep === 'precheck'">
        <div v-if="precheckLoading" class="flex justify-center py-8">
          <span class="ww-text-3">正在预检...</span>
        </div>
        <div v-else-if="precheckResult">
          <NAlert type="warning" title="操作说明" class="mb-3">
            退役将把该节点的全部数据迁移到其他节点后从集群中摘除。迁移期间该节点不再接受新写入。
          </NAlert>

          <div v-if="precheckResult.warnings.length" class="mb-3">
            <NAlert
              v-for="(w, i) in precheckResult.warnings"
              :key="i"
              type="error"
              class="mb-1"
              :title="w"
            />
          </div>

          <div class="mb-3 grid grid-cols-3 gap-3">
            <NStatistic label="缓存条目" :value="precheckResult.totalCacheItems" />
            <NStatistic label="存储对象" :value="precheckResult.totalStorageObjects" />
            <NStatistic label="主分区数" :value="precheckResult.totalPrimaryPartitions" />
          </div>

          <NDataTable
            :columns="[
              { title: '能力', key: 'type', width: 100 },
              { title: '期望态', key: 'desired', width: 100 },
              { title: '实际态', key: 'actual', width: 100 },
              { title: '主分区', key: 'primaryPartitions', width: 80 },
              { title: '副本分区', key: 'replicaPartitions', width: 90 },
              { title: '数据量', key: 'itemCount', width: 100 }
            ]"
            :data="precheckResult.capabilities"
            :bordered="false"
            size="small"
            :single-line="false"
            class="mb-3"
          />

          <div v-if="precheckResult.requiresForce" class="mb-3">
            <NCheckbox v-model:checked="forceConfirm">
              强制退役（无可用迁移目标，数据将被丢弃）
            </NCheckbox>
          </div>

          <div class="flex justify-end gap-2">
            <NButton @click="decommissionModalVisible = false">取消</NButton>
            <NButton
              type="error"
              :disabled="precheckResult.requiresForce && !forceConfirm"
              @click="handleDecommissionConfirm"
            >
              确认退役
            </NButton>
          </div>
        </div>
      </div>

      <div v-else-if="decommissionStep === 'progress'">
        <div v-if="!decommissionStatus" class="flex justify-center py-8">
          <span class="ww-text-3">正在查询进度...</span>
        </div>
        <div v-else>
          <NAlert
            v-if="decommissionStatus.record?.state === 'COMPLETED'"
            type="success"
            title="退役完成"
            class="mb-3"
          >
            节点已从集群中摘除，能力记录已清理。
          </NAlert>
          <NAlert
            v-else-if="decommissionStatus.record?.state === 'FAILED'"
            type="error"
            :title="'退役失败: ' + (decommissionStatus.record?.detail || '未知原因')"
            class="mb-3"
          />
          <NAlert
            v-else
            type="info"
            :title="'退役中: ' + (decommissionStatus.record?.state || 'DRAINING')"
            class="mb-3"
          >
            正在迁移数据，请耐心等待。
          </NAlert>

          <div v-if="decommissionStatus.record?.state !== 'COMPLETED'" class="mb-3">
            <div class="mb-1 text-sm ww-text-2">
              剩余分区: {{ decommissionStatus.totalPartitionsLeft }}
            </div>
            <NProgress
              v-if="precheckResult && precheckResult.totalPrimaryPartitions > 0"
              :percentage="Math.max(0, Math.min(100, Math.round((1 - decommissionStatus.totalPartitionsLeft / precheckResult.totalPrimaryPartitions) * 100)))"
              :show-indicator="true"
            />
          </div>

          <NDataTable
            v-if="decommissionStatus.capabilities.length"
            :columns="[
              { title: '能力', key: 'type', width: 100 },
              { title: '期望态', key: 'desired', width: 100 },
              { title: '实际态', key: 'actual', width: 100 },
              { title: '剩余主分区', key: 'primaryPartitionsLeft', width: 110 },
              { title: '剩余副本', key: 'replicaPartitionsLeft', width: 100 },
              { title: '活跃迁移', key: 'activeMigrationsFromNode', width: 90 }
            ]"
            :data="decommissionStatus.capabilities"
            :bordered="false"
            size="small"
            :single-line="false"
          />

          <div class="mt-3 flex justify-end">
            <NButton @click="decommissionModalVisible = false">
              {{ decommissionStatus.record?.state === 'COMPLETED' || decommissionStatus.record?.state === 'FAILED' ? '关闭' : '后台运行' }}
            </NButton>
          </div>
        </div>
      </div>
    </NModal>
  </div>
</template>
