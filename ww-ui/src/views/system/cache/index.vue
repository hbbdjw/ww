<script setup lang="ts">
import {
  computed,
  onBeforeUnmount,
  onMounted,
  ref,
  watch,
} from 'vue';

import { createIconifyIcon, Plus, RotateCw, Search } from '@vben/icons';

import {
  NButton,
  NCard,
  NEmpty,
  NIcon,
  NInput,
  NInputNumber,
  NPopconfirm,
  NSpace,
  NSpin,
  NTag,
  NTooltip,
  NTree,
} from 'naive-ui';

import { message } from '#/adapter/naive';
import {
  type CacheKeyEntry,
  type CacheNode,
  type CacheNodesOverview,
  type CacheValueInfo,
  deleteCacheKeyApi,
  formatTtl,
  getCacheKeysApi,
  getCacheNodesApi,
  getCacheValueApi,
  saveCacheValueApi,
  TTL_NEVER_EXPIRES,
} from '#/api/core/cache';
import { CodeEditor } from '#/components/CodeEditor';
import { WwModal } from '#/components/WwModal';

defineOptions({ name: 'SystemCache' });

// ==================== 图标组件 ====================
const ServerIcon = createIconifyIcon('lucide:server');
const TrashIcon = createIconifyIcon('lucide:trash-2');

// ==================== Key 树节点结构 ====================
interface KeyTreeNode {
  /** NTree 唯一标识（路径 + 叶子后缀） */
  id: string;
  /** 分段名称 */
  label: string;
  isLeaf: boolean;
  /** 叶子节点的完整 key */
  fullKey?: string;
  /** 叶子节点的持有节点 */
  nodeKey?: string;
  /** 分支下叶子数量 */
  count?: number;
  children?: KeyTreeNode[];
}

const ID_SEP = '\u0001';
const LEAF_SUFFIX = '\u0002';

// ==================== 状态 ====================
const overview = ref<CacheNodesOverview | null>(null);
const selectedNode = ref('');
const keyword = ref('');
const keyEntries = ref<CacheKeyEntry[]>([]);
const keysTotal = ref(0);
const keysTruncated = ref(false);
const loading = ref(false);
const nodesLoading = ref(false);
const treeData = ref<KeyTreeNode[]>([]);
const expandedKeys = ref<string[]>([]);
const selectedTreeKeys = ref<string[]>([]);
const nodeIdMap = new Map<string, KeyTreeNode>();

// 值查看/编辑
const showValueModal = ref(false);
const valueLoading = ref(false);
const savingValue = ref(false);
const currentValueInfo = ref<CacheValueInfo | null>(null);
const valueText = ref('');
const valueTtl = ref(0);
const valueEditorRef = ref<InstanceType<typeof CodeEditor>>();

/** Format the JSON value being edited */
function handleFormatValue() {
  void valueEditorRef.value?.format();
}

// 新增 key
const showCreateKey = ref(false);
const creatingKey = ref(false);
const newKey = ref('');
const newValue = ref('');
const newTtl = ref(0);

// ==================== 计算属性 ====================
const nodes = computed<CacheNode[]>(() => overview.value?.nodes ?? []);

const selectedNodeView = computed(() =>
  nodes.value.find((n) => n.nodeKey === selectedNode.value),
);

/** 头部统计：选中具体节点时展示节点视图，否则展示集群聚合 */
const headerStats = computed(() => {
  const node = selectedNodeView.value;
  if (node) {
    const s = node.stats;
    return {
      keyCount: s?.keyCount ?? 0,
      online: node.online ? '在线' : '离线',
      cpuLoad: node.online ? (s?.processCpuLoad ?? -1) : -1,
      heapUsedMb: s?.heapUsedMb ?? 0,
      heapMaxMb: s?.heapMaxMb ?? 0,
      heapUsedPercent: s?.heapUsedPercent ?? -1,
    };
  }
  const onlineNodes = nodes.value.filter((n) => n.online);
  const totalKeys = overview.value?.totalKeyCount ?? 0;
  let cpuSum = 0;
  let cpuCount = 0;
  let heapUsed = 0;
  let heapMax = 0;
  for (const n of onlineNodes) {
    const s = n.stats;
    if (!s) continue;
    if (s.processCpuLoad >= 0) {
      cpuSum += s.processCpuLoad;
      cpuCount++;
    }
    heapUsed += s.heapUsedMb;
    heapMax += s.heapMaxMb;
  }
  return {
    keyCount: totalKeys,
    online: `${overview.value?.onlineCount ?? 0} / ${nodes.value.length}`,
    cpuLoad: cpuCount > 0 ? cpuSum / cpuCount : -1,
    heapUsedMb: heapUsed,
    heapMaxMb: heapMax,
    heapUsedPercent: heapMax > 0 ? Math.round((heapUsed * 10000) / heapMax) / 100 : -1,
  };
});

function formatCpu(load: number | string): string {
  if (typeof load === 'string') return load;
  if (load < 0) return '-';
  return `${(load * 100).toFixed(1)}%`;
}

// ==================== 数据加载 ====================
async function fetchNodes() {
  nodesLoading.value = true;
  try {
    overview.value = await getCacheNodesApi();
    // 选中节点已不存在时回到全部节点
    if (
      selectedNode.value &&
      !nodes.value.some((n) => n.nodeKey === selectedNode.value)
    ) {
      selectedNode.value = '';
    }
  } catch {
    // error handled by interceptor
  } finally {
    nodesLoading.value = false;
  }
}

async function fetchKeys() {
  loading.value = true;
  try {
    const result = await getCacheKeysApi({
      nodeKey: selectedNode.value || undefined,
      keyword: keyword.value.trim() || undefined,
    });
    keyEntries.value = result.keys;
    keysTotal.value = result.total;
    keysTruncated.value = result.truncated;
    rebuildTree();
  } catch {
    // error handled by interceptor
  } finally {
    loading.value = false;
  }
}

async function refreshAll() {
  await Promise.all([fetchNodes(), fetchKeys()]);
}

watch(selectedNode, () => {
  selectedTreeKeys.value = [];
  fetchKeys();
});

// ==================== Key 树构建 ====================
/** 将扁平 key 列表按 ':' 分隔符构建为树（类 Redis 键空间浏览） */
function rebuildTree() {
  nodeIdMap.clear();
  const root: KeyTreeNode = {
    id: '',
    label: '',
    isLeaf: false,
    children: [],
  };
  for (const e of keyEntries.value) {
    const segs = e.key.split(':');
    let cur = root;
    let path = '';
    for (let i = 0; i < segs.length; i++) {
      const seg = segs[i]!;
      path = path ? `${path}${ID_SEP}${seg}` : seg;
      const isLast = i === segs.length - 1;
      let child = cur.children!.find(
        (c) => c.label === seg && c.isLeaf === isLast,
      );
      if (!child) {
        child = {
          id: isLast ? path + LEAF_SUFFIX : path,
          label: seg,
          isLeaf: isLast,
          fullKey: isLast ? e.key : undefined,
          nodeKey: isLast ? e.nodeKey : undefined,
          children: isLast ? undefined : [],
        };
        cur.children!.push(child);
      }
      cur = child;
    }
  }
  sortAndCount(root.children!);
  for (const child of root.children!) {
    nodeIdMap.set(child.id, child);
    collectNodeMap(child);
  }
  treeData.value = root.children!;
  expandedKeys.value = collectExpandedKeys();
}

function collectNodeMap(node: KeyTreeNode) {
  if (!node.children) return;
  for (const child of node.children) {
    nodeIdMap.set(child.id, child);
    collectNodeMap(child);
  }
}

/** 排序（分支在前）并统计分支下叶子数量，返回该层叶子总数 */
function sortAndCount(list: KeyTreeNode[]): number {
  let total = 0;
  for (const n of list) {
    if (n.children && n.children.length > 0) {
      n.count = sortAndCount(n.children);
      total += n.count;
    } else {
      n.count = 1;
      total += 1;
    }
  }
  list.sort((a, b) => {
    if (a.isLeaf !== b.isLeaf) return a.isLeaf ? 1 : -1;
    return a.label.localeCompare(b.label);
  });
  return total;
}

/** 默认展开策略：key 较少时全部展开，否则仅展开第一层 */
function collectExpandedKeys(): string[] {
  const expandAll = keyEntries.value.length <= 300;
  const keys: string[] = [];
  const walk = (list: KeyTreeNode[], depth: number) => {
    for (const n of list) {
      if (n.children && n.children.length > 0) {
        if (expandAll || depth === 1) keys.push(n.id);
        walk(n.children, depth + 1);
      }
    }
  };
  walk(treeData.value, 1);
  return keys;
}

// ==================== 树交互 ====================
function handleTreeSelect(keys: string[]) {
  if (keys.length === 0) return;
  const node = nodeIdMap.get(keys[0]!);
  if (node?.isLeaf && node.fullKey) {
    openValueModal(node.fullKey, node.nodeKey);
  }
}

// ==================== 值查看 / 编辑 ====================
async function openValueModal(key: string, nodeKey?: string) {
  showValueModal.value = true;
  valueLoading.value = true;
  currentValueInfo.value = null;
  valueText.value = '';
  valueTtl.value = 0;
  try {
    const info = await getCacheValueApi(key, nodeKey);
    currentValueInfo.value = info;
    if (info.exists) {
      valueText.value = formatValueToText(info.value);
      valueTtl.value =
        info.ttlMillis >= 0 && info.ttlMillis < TTL_NEVER_EXPIRES
          ? info.ttlMillis
          : 0;
    }
  } catch {
    // error handled by interceptor
  } finally {
    valueLoading.value = false;
  }
}

function formatValueToText(value: any): string {
  if (value === null || value === undefined) return '';
  if (typeof value === 'object') {
    try {
      return JSON.stringify(value, null, 2);
    } catch {
      return String(value);
    }
  }
  return String(value);
}

async function handleSaveCurrentValue() {
  const info = currentValueInfo.value;
  if (!info) return;
  savingValue.value = true;
  try {
    await saveCacheValueApi(info.key, valueText.value, valueTtl.value);
    message.success('保存成功');
    await openValueModal(info.key, info.nodeKey ?? undefined);
    await fetchKeys();
    await fetchNodes();
  } catch {
    // error handled by interceptor
  } finally {
    savingValue.value = false;
  }
}

async function handleDeleteCurrentKey() {
  const info = currentValueInfo.value;
  if (!info) return;
  try {
    await deleteCacheKeyApi(info.key);
    message.success('删除成功');
    showValueModal.value = false;
    await Promise.all([fetchKeys(), fetchNodes()]);
  } catch {
    // error handled by interceptor
  }
}

// ==================== 新增 Key ====================
function handleCreateKey() {
  newKey.value = '';
  newValue.value = '';
  newTtl.value = 0;
  showCreateKey.value = true;
}

async function handleSubmitCreateKey() {
  const key = newKey.value.trim();
  if (!key) {
    message.warning('请输入 Key');
    return false;
  }
  creatingKey.value = true;
  try {
    await saveCacheValueApi(key, newValue.value, newTtl.value);
    message.success('新增成功');
    showCreateKey.value = false;
    await Promise.all([fetchKeys(), fetchNodes()]);
  } catch {
    // error handled by interceptor
  } finally {
    creatingKey.value = false;
  }
  return true;
}

// ==================== 生命周期 ====================
onMounted(async () => {
  await fetchNodes();
  await fetchKeys();
});

onBeforeUnmount(() => {
  nodeIdMap.clear();
});


</script>

<template>
  <div class="cache-page flex h-full flex-col p-2">
    <NCard :bordered="false" class="flex-1 rounded-lg" :content-style="{ padding: '0' }">
      <div class="cache-layout">
        <!-- 左侧缓存节点列表 -->
        <div class="node-panel">
          <div class="node-header">
            <span class="node-title">缓存节点</span>
            <NTooltip>
              <template #trigger>
                <NButton
                  quaternary
                  size="small"
                  :loading="nodesLoading"
                  @click="refreshAll"
                >
                  <NIcon><RotateCw /></NIcon>
                </NButton>
              </template>
              刷新
            </NTooltip>
          </div>
          <div class="node-list">
            <!-- 全部节点 -->
            <div
              class="node-item"
              :class="{ active: selectedNode === '' }"
              @click="selectedNode = ''"
            >
              <ServerIcon :size="18" />
              <div class="node-info">
                <div class="node-name">全部节点</div>
                <div class="node-meta">
                  {{ overview?.totalKeyCount ?? 0 }} 个 Key
                </div>
              </div>
            </div>
            <!-- 单个节点 -->
            <div
              v-for="n in nodes"
              :key="n.nodeKey"
              class="node-item"
              :class="{ active: n.nodeKey === selectedNode }"
              @click="selectedNode = n.nodeKey"
            >
              <span
                class="online-dot"
                :class="{ online: n.online, offline: !n.online }"
              ></span>
              <div class="node-info">
                <div class="node-name">
                  {{ n.nodeKey }}
                  <NTag v-if="n.self" size="tiny" type="info" :bordered="false">
                    本机
                  </NTag>
                </div>
                <div class="node-meta">
                  {{ n.online ? `${n.stats?.keyCount ?? 0} 个 Key` : '离线' }}
                  · {{ n.status }}
                </div>
              </div>
            </div>
            <NEmpty
              v-if="nodes.length === 0 && !nodesLoading"
              description="暂无节点"
              :style="{ padding: '40px 0' }"
            />
          </div>
        </div>

        <!-- 右侧内容区 -->
        <div class="content-panel">
          <!-- 头部统计 + 工具栏 -->
          <div class="content-header">
            <div class="content-stats">
              <div class="stat-item">
                <div class="stat-value">{{ headerStats.keyCount }}</div>
                <div class="stat-label">Key 总数</div>
              </div>
              <div class="stat-divider"></div>
              <div class="stat-item">
                <div class="stat-value">{{ headerStats.online }}</div>
                <div class="stat-label">在线节点</div>
              </div>
              <div class="stat-divider"></div>
              <div class="stat-item">
                <div class="stat-value">
                  {{ formatCpu(headerStats.cpuLoad) }}
                </div>
                <div class="stat-label">CPU</div>
              </div>
              <div class="stat-divider"></div>
              <div class="stat-item">
                <div class="stat-value">
                  {{ headerStats.heapUsedMb }} /
                  {{ headerStats.heapMaxMb }} MB
                </div>
                <div class="stat-label">
                  堆内存{{
                    headerStats.heapUsedPercent >= 0
                      ? `（${headerStats.heapUsedPercent}%）`
                      : ''
                  }}
                </div>
              </div>
            </div>
            <div class="content-toolbar">
              <NButton
                type="primary"
                size="small"
                @click="handleCreateKey"
              >
                <template #icon><NIcon><Plus /></NIcon></template>
                新增 Key
              </NButton>
              <NButton
                size="small"
                :loading="loading"
                @click="fetchKeys"
              >
                <template #icon><NIcon><RotateCw /></NIcon></template>
                刷新
              </NButton>
            </div>
          </div>

          <!-- 搜索栏 -->
          <div class="search-bar">
            <NInput
              v-model:value="keyword"
              placeholder="搜索 Key（支持模糊匹配，如 user 或 shiro:session）"
              clearable
              size="small"
              @keyup.enter="fetchKeys"
            >
              <template #prefix>
                <NIcon :component="Search" />
              </template>
            </NInput>
            <NButton type="primary" size="small" @click="fetchKeys">
              搜索
            </NButton>
          </div>

          <!-- Key 树形列表 -->
          <div class="key-content">
            <NSpin :show="loading">
              <template v-if="!loading && keyEntries.length === 0">
                <NEmpty
                  :description="
                    keyword.trim() ? '未找到匹配的 Key' : '暂无缓存数据'
                  "
                  :style="{ padding: '60px 0' }"
                />
              </template>
              <template v-else>
                <div class="key-summary">
                  共 {{ keysTotal }} 个 Key
                  <template v-if="keysTruncated">
                    （已截断显示，请用搜索缩小范围）
                  </template>
                </div>
                <NTree
                  :data="treeData"
                  block-line
                  expand-on-click
                  selectable
                  key-field="id"
                  label-field="label"
                  children-field="children"
                  :expanded-keys="expandedKeys"
                  :selected-keys="selectedTreeKeys"
                  @update:expanded-keys="(k: string[]) => (expandedKeys = k)"
                  @update:selected-keys="(k: string[]) => handleTreeSelect(k)"
                />
              </template>
            </NSpin>
          </div>
        </div>
      </div>
    </NCard>

    <!-- 值查看/编辑弹窗 -->
    <WwModal
      v-model:show="showValueModal"
      title="缓存值详情"
      :width="640"
      :mask-closable="false"
      :footer="false"
    >
      <NSpin :show="valueLoading">
        <div v-if="currentValueInfo" class="value-detail">
          <div class="value-meta">
            <div class="meta-row">
              <span class="meta-label">Key</span>
              <span class="meta-value mono">
                {{ currentValueInfo.key }}
              </span>
            </div>
            <div class="meta-row">
              <span class="meta-label">持有节点</span>
              <span class="meta-value">
                {{ currentValueInfo.nodeKey || '-' }}
              </span>
            </div>
            <div class="meta-row">
              <span class="meta-label">剩余 TTL</span>
              <span class="meta-value">
                {{ formatTtl(currentValueInfo.ttlMillis) }}
              </span>
            </div>
          </div>

          <template v-if="currentValueInfo.exists">
            <div class="value-edit-label">
              Value（支持 JSON 文本，保存时自动解析）
            </div>
            <CodeEditor
              ref="valueEditorRef"
              v-model="valueText"
              language="json"
              height="320px"
            />
            <div class="value-edit-label">
              过期时间 TTL（毫秒，0 表示永不过期）
            </div>
            <NInputNumber
              v-model:value="valueTtl"
              :min="0"
              :step="1000"
              style="width: 100%"
              placeholder="0 表示永不过期"
            />
            <div class="value-actions">
              <NSpace>
                <NButton @click="handleFormatValue">
                  格式化
                </NButton>
                <NButton
                  type="primary"
                  :loading="savingValue"
                  @click="handleSaveCurrentValue"
                >
                  保存
                </NButton>
                <NPopconfirm @positive-click="handleDeleteCurrentKey">
                  <template #trigger>
                    <NButton type="error">
                      <template #icon><NIcon><TrashIcon /></NIcon></template>
                      删除 Key
                    </NButton>
                  </template>
                  确定删除 Key「{{ currentValueInfo.key }}」吗？
                </NPopconfirm>
              </NSpace>
            </div>
          </template>
          <NEmpty v-else description="该 Key 不存在或已过期" />
        </div>
      </NSpin>
    </WwModal>

    <!-- 新增 Key 弹窗 -->
    <WwModal
      v-model:show="showCreateKey"
      title="新增 Key"
      :width="520"
      :mask-closable="false"
      confirm-text="创建"
      cancel-text="取消"
      :confirm-loading="creatingKey"
      :maximizable="false"
      :resizable="false"
      @confirm="handleSubmitCreateKey"
    >
      <div>
        <div class="value-edit-label">Key</div>
        <NInput
          v-model:value="newKey"
          placeholder="如 user:1:profile，冒号用于树形分组"
        />
        <div class="value-edit-label">
          Value（支持 JSON 文本，保存时自动解析）
        </div>
        <NInput
          v-model:value="newValue"
          type="textarea"
          class="mono"
          placeholder="请输入值"
          :autosize="{ minRows: 4, maxRows: 10 }"
        />
        <div class="value-edit-label">
          过期时间 TTL（毫秒，0 表示永不过期）
        </div>
        <NInputNumber
          v-model:value="newTtl"
          :min="0"
          :step="1000"
          style="width: 100%"
          placeholder="0 表示永不过期"
        />
      </div>
    </WwModal>
  </div>
</template>

<style scoped>
.cache-layout {
  display: flex;
  height: 100%;
  min-height: 600px;
}

.node-panel {
  width: 260px;
  min-width: 260px;
  border-right: 1px solid var(--n-border-color, #e5e7eb);
  display: flex;
  flex-direction: column;
}

.node-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 12px 16px;
  border-bottom: 1px solid var(--n-border-color, #e5e7eb);
}

.node-title {
  font-weight: 600;
  font-size: 14px;
}

.node-list {
  flex: 1;
  overflow-y: auto;
  padding: 4px;
}

.node-item {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 12px;
  border-radius: 6px;
  cursor: pointer;
  transition: background-color 0.15s;
}

.node-item:hover {
  background-color: var(--n-color-hover, rgba(0, 0, 0, 0.04));
}

.node-item.active {
  background-color: rgba(24, 160, 240, 0.2);
}

.node-info {
  flex: 1;
  min-width: 0;
}

.node-name {
  font-size: 13px;
  font-weight: 500;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  display: flex;
  align-items: center;
  gap: 4px;
}

.node-meta {
  font-size: 11px;
  color: var(--n-text-color-3, #999);
}

.online-dot {
  width: 8px;
  height: 8px;
  min-width: 8px;
  border-radius: 50%;
}

.online-dot.online {
  background-color: #18a058;
}

.online-dot.offline {
  background-color: #d03050;
}

.content-panel {
  flex: 1;
  display: flex;
  flex-direction: column;
  min-width: 0;
}

.content-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 16px 20px;
  border-bottom: 1px solid var(--n-border-color, #e5e7eb);
  flex-wrap: wrap;
  gap: 8px;
}

.content-stats {
  display: flex;
  align-items: center;
  gap: 24px;
  flex-wrap: wrap;
}

.stat-item {
  display: flex;
  flex-direction: column;
}

.stat-value {
  font-size: 18px;
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

.content-toolbar {
  display: flex;
  align-items: center;
  gap: 4px;
}

.search-bar {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 12px;
  border-bottom: 1px solid var(--n-border-color, #e5e7eb);
  background-color: var(--n-color-modal, #fafafa);
}

.key-content {
  flex: 1;
  min-height: 0;
  overflow: auto;
  padding: 8px 12px;
}

.key-summary {
  font-size: 12px;
  color: var(--n-text-color-3, #999);
  padding: 4px 0 8px;
}

.mono {
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
}

.value-detail {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.value-meta {
  background-color: var(--n-color-modal, #fafafa);
  border-radius: 6px;
  padding: 8px 12px;
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.meta-row {
  display: flex;
  gap: 8px;
  font-size: 12px;
}

.meta-label {
  color: var(--n-text-color-3, #999);
  min-width: 64px;
}

.meta-value {
  word-break: break-all;
}

.value-edit-label {
  font-size: 12px;
  color: var(--n-text-color-2, #666);
  margin-top: 4px;
}

.value-actions {
  display: flex;
  justify-content: flex-end;
  padding-top: 4px;
}
</style>
