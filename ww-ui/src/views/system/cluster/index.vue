<script setup lang="ts">
import { computed, h, onBeforeUnmount, onMounted, ref } from 'vue';

import { createIconifyIcon, RotateCw } from '@vben/icons';

import {
  NAlert,
  NButton,
  NCard,
  NDataTable,
  NDescriptions,
  NDescriptionsItem,
  NForm,
  NFormItem,
  NIcon,
  NInput,
  NInputNumber,
  NPopconfirm,
  NSelect,
  NSpace,
  NSwitch,
  NTag,
} from 'naive-ui';
import type { DataTableColumns } from 'naive-ui';

import { message } from '#/adapter/naive';
import {
  type ClusterNodeView,
  type ClusterStatusOverview,
  type ClusterStatusStreamHandle,
  type ClusterStreamSnapshot,
  type StorageClusterHealth,
  type StorageClusterTopology,
  clusterCancelFailoverApi,
  clusterFailoverApi,
  clusterJoinApi,
  clusterLeaveApi,
  clusterRebalanceApi,
  clusterTransferMasterApi,
  startClusterStatusStreamApi,
  storageMaintenanceApi,
  storageMergeApi,
  storageScaleOutApi,
  storageTransferApi,
  toggleNodeCapabilityApi,
} from '#/api/core/cluster';
import { WwModal } from '#/components/WwModal';

import { mergeClusterNodes, type UnifiedNode } from './unified-nodes';

defineOptions({ name: 'SystemCluster' });

// ==================== 图标 ====================
const PlusIcon = createIconifyIcon('lucide:plus');

// ==================== 通用状态 ====================
const loading = ref(false);

// ==================== SSE 实时推送 ====================
/** SSE 连接状态：true=实时推送中，false=断连重连中 */
const streamConnected = ref(false);
let streamHandle: ClusterStatusStreamHandle | null = null;
/** 最近一次 SSE 快照时间戳（顶部展示，证明数据在实时刷新） */
const lastSnapshotTime = ref(0);

function applySnapshot(snap: ClusterStreamSnapshot) {
  lastSnapshotTime.value = snap.timestamp;
  if (snap.overview && !snap.overview.error) {
    overviewData.value = snap.overview;
  }
  if (snap.cacheNodes && Array.isArray(snap.cacheNodes.nodes)) {
    cacheNodes.value = snap.cacheNodes.nodes;
    cacheSelfKey.value = snap.cacheNodes.selfNodeKey || '';
  }
  const st = snap.storage;
  if (st) {
    storageMaintenance.value = st.maintenance === true;
    storageEnabled.value = st.enabled === true;
    if (st.topology && Array.isArray(st.topology.nodes)) {
      storageTopology.value = st.topology;
    }
    if (st.health && Array.isArray(st.health.nodes)) {
      storageHealth.value = st.health;
    }
  }
}

function startStream() {
  stopStream();
  streamHandle = startClusterStatusStreamApi(applySnapshot, (connected) => {
    streamConnected.value = connected;
  });
}

function stopStream() {
  if (streamHandle) {
    streamHandle.stop();
    streamHandle = null;
  }
  streamConnected.value = false;
}

// ==================== 数据源 ====================
const overviewData = ref<ClusterStatusOverview | null>(null);
const cacheNodes = ref<ClusterNodeView[]>([]);
const cacheSelfKey = ref('');
const storageTopology = ref<StorageClusterTopology | null>(null);
const storageHealth = ref<StorageClusterHealth | null>(null);
const storageMaintenance = ref(false);
/** 存储集群是否启用（topologyManager 不为空） */
const storageEnabled = ref(false);

/** failover 中处于手动隔离（manualDead）的节点数 */
const manualDeadCount = computed(() => {
  const nodes = overviewData.value?.failover?.nodes;
  if (!Array.isArray(nodes)) return 0;
  return nodes.filter((n: any) => n.manualDead).length;
});

// ==================== 统一节点表 ====================
/**
 * 把来自三个数据源的节点合并成一行：
 * overview（membership 状态/epoch/心跳）、cacheNodes（在线/指标）、
 * storageTopology（存储端口/权重）、storageHealth（存储健康/失败次数）。
 *
 * 缓存节点键为 host:cachePort，存储节点键为 host:httpPort，二者标识同一物理节点；
 * 存储侧数据附带 cacheNodeKey，合并时以 cacheNodeKey ?? nodeKey 对齐（见 unified-nodes.ts），
 * 存储专属操作的 host:httpPort 键保留在 storageNodeKey 字段。
 */

const unifiedNodes = computed<UnifiedNode[]>(() =>
  mergeClusterNodes({
    overview: overviewData.value?.nodes ?? [],
    cacheNodes: cacheNodes.value,
    storageTopology: storageTopology.value?.nodes ?? [],
    storageHealth: storageHealth.value?.nodes ?? [],
  }),
);

const unifiedColumns: DataTableColumns<UnifiedNode> = [
  { title: '节点', key: 'nodeKey', width: 170, ellipsis: { tooltip: true } },
  { title: 'Host', key: 'host', width: 130 },
  { title: '缓存端口', key: 'cachePort', width: 90, render: (r) => r.cachePort || '-' },
  { title: 'HTTP 端口', key: 'httpPort', width: 100, render: (r) => r.httpPort || '-' },
  {
    title: '缓存状态', key: 'cacheStatus', width: 100,
    render(row) {
      if (row.cacheStatus === '-') return '-';
      const typeMap: Record<string, 'error' | 'success' | 'warning' | 'default'> = {
        ALIVE: 'success', SUSPECT: 'warning', DEAD: 'error',
      };
      return h(NTag, { type: typeMap[row.cacheStatus] || 'default', size: 'small' }, { default: () => row.cacheStatus });
    },
  },
  {
    title: '在线', key: 'online', width: 70,
    render(row) {
      if (!row.hasCache) return '-';
      return h(NTag, { type: row.online ? 'success' : 'error', size: 'small' }, { default: () => row.online ? '在线' : '离线' });
    },
  },
  {
    title: 'CPU', key: 'cpu', width: 80,
    render(row) {
      const cpu = row.metrics?.cpu;
      return cpu ? `${(cpu.processCpuLoad * 100).toFixed(1)}%` : '-';
    },
  },
  {
    title: '堆内存', key: 'mem', width: 130,
    render(row) {
      const mem = row.metrics?.memory;
      return mem ? `${mem.heapUsedMb}MB / ${mem.heapMaxMb}MB` : '-';
    },
  },
  {
    title: 'Key 数', key: 'keys', width: 80,
    render(row) {
      return row.metrics?.cache?.keyCount ?? '-';
    },
  },
  {
    title: '存储健康', key: 'storageHealth', width: 110,
    render(row) {
      if (row.storageStatus === '-') return '-';
      const typeMap: Record<string, 'error' | 'success' | 'warning' | 'default'> = {
        ALIVE: 'success', SUSPECT: 'warning', DEAD: 'error', QUARANTINE: 'warning',
      };
      return h(NTag, { type: typeMap[row.storageStatus] || 'default', size: 'small' }, { default: () => row.storageStatus });
    },
  },
  {
    title: '存储', key: 'storageEnabled', width: 70,
    render(row) {
      return h(NSwitch, {
        value: row.storageEnabled, size: 'small',
        onUpdateValue: (v: boolean) => handleToggleCapability(row.nodeKey, 'storage', v),
      });
    },
  },
  {
    title: '缓存', key: 'cacheEnabled', width: 70,
    render(row) {
      return h(NSwitch, {
        value: row.cacheEnabled, size: 'small',
        onUpdateValue: (v: boolean) => handleToggleCapability(row.nodeKey, 'cache', v),
      });
    },
  },
  {
    title: 'Self', key: 'self', width: 70,
    render(row) {
      return row.self ? h(NTag, { type: 'info', size: 'small' }, { default: () => 'Self' }) : '-';
    },
  },
  {
    title: '操作', key: 'actions', width: 260, fixed: 'right',
    render(row) {
      if (row.self) return h('span', { class: 'text-gray-400 text-xs' }, '当前节点');
      const btns: any[] = [];
      // 缓存节点操作
      if (row.hasCache && row.cacheStatus !== '-') {
        if (row.cacheStatus !== 'ALIVE') {
          btns.push(h(NButton, { size: 'small', type: 'success', quaternary: true, onClick: () => handleMarkAlive(row.nodeKey) }, { default: () => '恢复' }));
        } else {
          btns.push(h(NPopconfirm, { onPositiveClick: () => handleFailover(row.nodeKey) }, {
            trigger: () => h(NButton, { size: 'small', type: 'warning', quaternary: true }, { default: () => '隔离' }),
            default: () => `确定将节点 ${row.nodeKey} 标记为离线？`,
          }));
          btns.push(h(NButton, { size: 'small', type: 'primary', quaternary: true, onClick: () => openTransferModal(row.nodeKey) }, { default: () => '迁移' }));
          btns.push(h(NPopconfirm, { onPositiveClick: () => handleLeave(row.host, row.cachePort) }, {
            trigger: () => h(NButton, { size: 'small', type: 'error', quaternary: true }, { default: () => '退出' }),
            default: () => `确定让节点 ${row.nodeKey} 退出集群？`,
          }));
        }
      }
      // 存储节点操作（存储 API 使用 host:httpPort 节点键）
      if (row.hasStorage) {
        const storageKey = row.storageNodeKey ?? row.nodeKey;
        btns.push(h(NButton, { size: 'small', type: 'warning', quaternary: true, onClick: () => openStorageTransferModal(storageKey) }, { default: () => '迁移' }));
        btns.push(h(NButton, { size: 'small', type: 'error', quaternary: true, onClick: () => openStorageMergeModal(storageKey) }, { default: () => '合并' }));
      }
      return h(NSpace, { size: 4 }, { default: () => btns });
    },
  },
];

// ==================== 操作函数 ====================
async function handleToggleCapability(nodeKey: string, capability: 'cache' | 'storage', enabled: boolean) {
  try {
    await toggleNodeCapabilityApi(nodeKey, capability, enabled);
    message.success(`已${enabled ? '启用' : '禁用'} ${capability} 能力`);
  } catch (e: any) {
    message.error(e?.message || '切换失败');
  }
}

async function handleMarkAlive(nodeKey: string) {
  try {
    await clusterCancelFailoverApi(nodeKey);
    message.success('节点已恢复为 ALIVE');
  } catch (e: any) {
    message.error(e?.message || '操作失败');
  }
}

async function handleFailover(nodeKey: string) {
  try {
    await clusterFailoverApi(nodeKey);
    message.success('节点已标记为离线');
  } catch (e: any) {
    message.error(e?.message || '操作失败');
  }
}

async function handleLeave(host: string, port: number) {
  try {
    await clusterLeaveApi(host, port);
    message.success('节点已退出集群');
  } catch (e: any) {
    message.error(e?.message || '操作失败');
  }
}

async function handleRebalance() {
  try {
    await clusterRebalanceApi();
    message.success('重平衡已触发');
  } catch (e: any) {
    message.error(e?.message || '操作失败');
  }
}

async function handleToggleMaintenance(enabled: boolean) {
  try {
    await storageMaintenanceApi(enabled);
    message.success(`维护态已${enabled ? '开启' : '关闭'}`);
    storageMaintenance.value = enabled;
  } catch (e: any) {
    message.error(e?.message || '操作失败');
  }
}

// ==================== 弹窗：缓存节点加入 ====================
const showJoinModal = ref(false);
const joinForm = ref({ host: '', port: 9000, weight: 1.0 });

function openJoinModal() {
  joinForm.value = { host: '', port: 9000, weight: 1.0 };
  showJoinModal.value = true;
}

async function handleJoin() {
  if (!joinForm.value.host) { message.warning('请输入 Host'); return; }
  if (!joinForm.value.port) { message.warning('请输入 Port'); return; }
  try {
    await clusterJoinApi(joinForm.value.host, joinForm.value.port, joinForm.value.weight);
    message.success('节点已加入集群');
    showJoinModal.value = false;
  } catch (e: any) {
    message.error(e?.message || '加入失败');
  }
}

// ==================== 弹窗：缓存节点迁移 ====================
const showTransferModal = ref(false);
const transferSource = ref('');
const transferTarget = ref('');

const transferTargetOptions = computed(() =>
  cacheNodes.value
    .filter((n) => n.nodeKey !== transferSource.value && n.online)
    .map((n) => ({ label: `${n.nodeKey}${n.self ? ' (本节点)' : ''}`, value: n.nodeKey })),
);

function openTransferModal(sourceNodeKey: string) {
  transferSource.value = sourceNodeKey;
  transferTarget.value = '';
  showTransferModal.value = true;
}

async function handleTransfer() {
  try {
    await clusterTransferMasterApi(transferSource.value, transferTarget.value || undefined);
    message.success('迁移已触发');
    showTransferModal.value = false;
  } catch (e: any) {
    message.error(e?.message || '迁移失败');
  }
}

// ==================== 弹窗：存储扩容 ====================
const showScaleOutModal = ref(false);
const scaleOutForm = ref({ host: '', httpPort: 8080, weight: 1.0 });

function openScaleOutModal() {
  scaleOutForm.value = { host: '', httpPort: 8080, weight: 1.0 };
  showScaleOutModal.value = true;
}

async function handleScaleOut() {
  if (!scaleOutForm.value.host) { message.warning('请输入 Host'); return; }
  try {
    await storageScaleOutApi(scaleOutForm.value.host, scaleOutForm.value.httpPort, scaleOutForm.value.weight);
    message.success('扩容已触发');
    showScaleOutModal.value = false;
  } catch (e: any) {
    message.error(e?.message || '扩容失败');
  }
}

// ==================== 弹窗：存储迁移 ====================
const showStorageTransferModal = ref(false);
const storageTransferNodeKey = ref('');

function openStorageTransferModal(nodeKey: string) {
  storageTransferNodeKey.value = nodeKey;
  showStorageTransferModal.value = true;
}

async function handleStorageTransfer() {
  try {
    await storageTransferApi(storageTransferNodeKey.value);
    message.success('存储迁移已触发');
    showStorageTransferModal.value = false;
  } catch (e: any) {
    message.error(e?.message || '迁移失败');
  }
}

// ==================== 弹窗：存储合并 ====================
const showStorageMergeModal = ref(false);
const storageMergeNodeKey = ref('');
const storageMergePurge = ref(true);

function openStorageMergeModal(nodeKey: string) {
  storageMergeNodeKey.value = nodeKey;
  storageMergePurge.value = true;
  showStorageMergeModal.value = true;
}

async function handleStorageMerge() {
  try {
    await storageMergeApi(storageMergeNodeKey.value, storageMergePurge.value);
    message.success('存储合并已触发');
    showStorageMergeModal.value = false;
  } catch (e: any) {
    message.error(e?.message || '合并失败');
  }
}

// ==================== 生命周期 ====================
onMounted(() => {
  startStream();
});

onBeforeUnmount(() => {
  stopStream();
});
</script>

<template>
  <div class="flex h-full flex-col p-4">
    <!-- SSE 实时推送状态栏 -->
    <div class="mb-3 flex items-center justify-between">
      <NSpace :size="8" align="center">
        <NTag :type="streamConnected ? 'success' : 'warning'" size="small" round>
          {{ streamConnected ? '实时推送中' : '连接断开，重连中…' }}
        </NTag>
        <span v-if="lastSnapshotTime" class="text-xs text-gray-400">
          最后更新: {{ new Date(lastSnapshotTime).toLocaleTimeString() }}
        </span>
        <NTag v-if="cacheSelfKey" size="small">本节点: {{ cacheSelfKey }}</NTag>
        <NTag v-if="storageEnabled" :type="storageMaintenance ? 'error' : 'success'" size="small">
          存储维护态: {{ storageMaintenance ? '开启' : '关闭' }}
        </NTag>
      </NSpace>
      <NSpace :size="6">
        <NButton size="tiny" quaternary @click="startStream">重建连接</NButton>
        <NPopconfirm @positive-click="handleRebalance">
          <template #trigger>
            <NButton size="small" type="warning">
              <template #icon><NIcon :component="RotateCw" /></template>
              触发重平衡
            </NButton>
          </template>
          确定触发数据重平衡？将扫描本地缓存并推送不属于本节点的 key。
        </NPopconfirm>
        <NSwitch :value="storageMaintenance" @update:value="handleToggleMaintenance">
          <template #checked>维护中</template>
          <template #unchecked>正常</template>
        </NSwitch>
        <NButton size="small" type="primary" @click="openScaleOutModal">
          <template #icon><NIcon :component="PlusIcon" /></template>
          扩容存储
        </NButton>
        <NButton size="small" type="primary" @click="openJoinModal">
          <template #icon><NIcon :component="PlusIcon" /></template>
          加入缓存节点
        </NButton>
      </NSpace>
    </div>

    <!-- 未启用动态成员管理提示 -->
    <NAlert
      v-if="overviewData && overviewData.membershipEnabled === false"
      type="warning"
      :bordered="false"
      class="mb-3"
    >
      集群动态成员管理未启用（cluster.membership-enabled=false），当前为静态 cluster.nodes 视图；
      加入/退出/failover 等动态操作不可用，请开启配置后重启服务。
    </NAlert>

    <!-- 统计卡片 -->
    <div class="mb-3 grid grid-cols-2 gap-3 lg:grid-cols-4">
      <NCard size="small">
        <NDescriptions label-placement="left" :column="1" size="small">
          <NDescriptionsItem label="拓扑版本">
            <NTag type="info">{{ overviewData?.topologyVersion ?? '-' }}</NTag>
          </NDescriptionsItem>
        </NDescriptions>
      </NCard>
      <NCard size="small">
        <NDescriptions label-placement="left" :column="1" size="small">
          <NDescriptionsItem label="存活节点">
            <NTag type="success">{{ overviewData?.aliveCount ?? '-' }}</NTag>
            <span class="ml-1 text-xs text-gray-400">/ {{ unifiedNodes.length }}</span>
          </NDescriptionsItem>
        </NDescriptions>
      </NCard>
      <NCard size="small">
        <NDescriptions label-placement="left" :column="1" size="small">
          <NDescriptionsItem label="Failover">
            <NTag :type="overviewData?.failover?.enabled === false ? 'default' : (manualDeadCount > 0 ? 'warning' : 'success')" size="small">
              {{ overviewData?.failover?.enabled === false ? '未启用' : (manualDeadCount > 0 ? `手动隔离 ${manualDeadCount} 节点` : '正常') }}
            </NTag>
          </NDescriptionsItem>
        </NDescriptions>
      </NCard>
      <NCard size="small">
        <NDescriptions label-placement="left" :column="1" size="small">
          <NDescriptionsItem label="Rebalance">
            <NTag :type="overviewData?.rebalance?.enabled === false ? 'default' : 'success'" size="small">
              {{ overviewData?.rebalance?.enabled === false ? '未启用' : (overviewData?.rebalance?.inProgress ? '进行中' : '正常') }}
            </NTag>
            <span v-if="overviewData?.rebalance?.totalMigratedKeys !== undefined" class="ml-2 text-xs text-gray-400">
              累计 {{ overviewData?.rebalance?.totalMigratedKeys }} keys
            </span>
          </NDescriptionsItem>
        </NDescriptions>
      </NCard>
    </div>

    <!-- 统一节点表（主内容区） -->
    <NCard title="集群节点" size="small" class="flex-1 overflow-hidden">
      <NDataTable
        :columns="unifiedColumns"
        :data="unifiedNodes"
        :bordered="false"
        :single-line="false"
        size="small"
        :loading="loading"
        :scroll-x="1600"
        max-height="100%"
        virtual-scroll
      />
    </NCard>

    <!-- ==================== 弹窗们 ==================== -->

    <!-- 缓存节点加入 -->
    <WwModal
      v-model:show="showJoinModal"
      title="加入缓存节点"
      :width="460"
      :mask-closable="false"
      confirm-text="加入"
      cancel-text="取消"
      :maximizable="false"
      :resizable="false"
      @confirm="handleJoin"
    >
      <NForm label-placement="left" label-width="80px">
        <NFormItem label="Host" required>
          <NInput v-model:value="joinForm.host" placeholder="如 192.168.1.100" />
        </NFormItem>
        <NFormItem label="Port" required>
          <NInputNumber v-model:value="joinForm.port" :min="1" :max="65535" placeholder="节点 TCP 端口" style="width: 100%" />
        </NFormItem>
        <NFormItem label="权重">
          <NInputNumber v-model:value="joinForm.weight" :min="0.1" :step="0.1" :precision="1" style="width: 100%" />
        </NFormItem>
      </NForm>
    </WwModal>

    <!-- 缓存节点迁移 -->
    <WwModal
      v-model:show="showTransferModal"
      title="迁移缓存节点"
      :width="480"
      :mask-closable="false"
      confirm-text="开始迁移"
      cancel-text="取消"
      :maximizable="false"
      :resizable="false"
      @confirm="handleTransfer"
    >
      <NForm label-placement="left" label-width="100px">
        <NFormItem label="源节点">
          <NTag>{{ transferSource }}</NTag>
        </NFormItem>
        <NFormItem label="目标节点">
          <NSelect
            v-model:value="transferTarget"
            :options="[{ label: '自动（由 HRW 决定）', value: '' }, ...transferTargetOptions]"
            placeholder="选择目标节点"
            clearable
          />
        </NFormItem>
      </NForm>
      <p class="mt-2 text-xs text-gray-500">
        源节点将被标记为离线，其数据由存活节点接管。目标为空时由 HRW 算法自动选择。
      </p>
    </WwModal>

    <!-- 存储扩容 -->
    <WwModal
      v-model:show="showScaleOutModal"
      title="扩容存储节点"
      :width="460"
      :mask-closable="false"
      confirm-text="扩容"
      cancel-text="取消"
      :maximizable="false"
      :resizable="false"
      @confirm="handleScaleOut"
    >
      <NForm label-placement="left" label-width="100px">
        <NFormItem label="Host" required>
          <NInput v-model:value="scaleOutForm.host" placeholder="新节点 Host" />
        </NFormItem>
        <NFormItem label="HTTP 端口" required>
          <NInputNumber v-model:value="scaleOutForm.httpPort" :min="1" :max="65535" style="width: 100%" />
        </NFormItem>
        <NFormItem label="权重">
          <NInputNumber v-model:value="scaleOutForm.weight" :min="0.1" :step="0.1" :precision="1" style="width: 100%" />
        </NFormItem>
      </NForm>
      <p class="mt-2 text-xs text-gray-500">
        扩容前请先开启维护态。新节点需已启动并通过配置加入集群。
      </p>
    </WwModal>

    <!-- 存储迁移 -->
    <WwModal
      v-model:show="showStorageTransferModal"
      title="迁移存储节点数据"
      :width="460"
      :mask-closable="false"
      confirm-text="开始迁移"
      cancel-text="取消"
      :maximizable="false"
      :resizable="false"
      @confirm="handleStorageTransfer"
    >
      <NForm label-placement="left" label-width="100px">
        <NFormItem label="源节点">
          <NTag>{{ storageTransferNodeKey }}</NTag>
        </NFormItem>
      </NForm>
      <p class="mt-2 text-xs text-gray-500">
        将源节点数据迁移到其他节点，源节点保留在集群中。操作前请先开启维护态。
      </p>
    </WwModal>

    <!-- 存储合并 -->
    <WwModal
      v-model:show="showStorageMergeModal"
      title="合并存储节点"
      :width="460"
      :mask-closable="false"
      confirm-text="开始合并"
      cancel-text="取消"
      :maximizable="false"
      :resizable="false"
      @confirm="handleStorageMerge"
    >
      <NForm label-placement="left" label-width="100px">
        <NFormItem label="源节点">
          <NTag>{{ storageMergeNodeKey }}</NTag>
        </NFormItem>
        <NFormItem label="清理源端">
          <NSwitch v-model:value="storageMergePurge" />
          <span class="ml-2 text-xs text-gray-500">{{ storageMergePurge ? '迁移后删除源端数据' : '迁移后保留源端数据' }}</span>
        </NFormItem>
      </NForm>
      <p class="mt-2 text-xs text-gray-500">
        将源节点数据合并到其他节点并从集群移除。操作前请先开启维护态。
      </p>
    </WwModal>
  </div>
</template>
