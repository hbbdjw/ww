<script setup lang="ts">
import { computed, h, onMounted, ref } from 'vue'
import {
  NButton,
  NCard,
  NEmpty,
  NForm,
  NFormItem,
  NInput,
  NInputNumber,
  NPopconfirm,
  NRadio,
  NRadioGroup,
  NSpace,
  NSpin,
  NTag,
  NTree,
  useMessage,
  type TreeOption
} from 'naive-ui'
import WwModal from '@/components/WwModal.vue'
import {
  type CacheKeyEntry,
  type CacheValueInfo,
  type CacheValueType,
  deleteCacheKeyApi,
  deleteCachePrefixApi,
  formatTtl,
  getCacheKeysApi,
  getCacheValueApi,
  saveCacheValueApi,
  TTL_NEVER_EXPIRES
} from '@/api/cache'
import JsonEditor from '@/components/JsonEditor.vue'

defineOptions({ name: 'CacheManage' })

const message = useMessage()

// ==================== 数据状态 ====================
const prefix = ref('')
const limit = ref(2000)
const keyEntries = ref<CacheKeyEntry[]>([])
const keysTotal = ref(0)
const truncated = ref(false)
const loading = ref(false)

// ==================== 值弹窗 ====================
const showValueModal = ref(false)
const valueLoading = ref(false)
const savingValue = ref(false)
const currentKey = ref('')
const currentValueInfo = ref<CacheValueInfo | null>(null)
const valueText = ref('')
const valueMode = ref<CacheValueType>('text')
const valueEditorRef = ref<InstanceType<typeof JsonEditor> | null>(null)

// ==================== 新增弹窗 ====================
const showCreateModal = ref(false)
const creatingKey = ref(false)
const newKey = ref('')
const newValue = ref('')
const newTtl = ref(0)
const newMode = ref<CacheValueType>('text')
const newEditorRef = ref<InstanceType<typeof JsonEditor> | null>(null)

// ==================== 树构建 ====================
interface TreeNode extends TreeOption {
  key: string
  label: string
  isLeaf: boolean
  fullKey?: string
  ttlMillis?: number
  prefixPath?: string
  childCount?: number
  children?: TreeNode[]
}

const treeData = computed<TreeNode[]>(() => buildKeyTree(keyEntries.value))

function buildKeyTree(entries: CacheKeyEntry[]): TreeNode[] {
  const root: Map<string, any> = new Map()

  for (const entry of entries) {
    const segments = entry.key.split(':')
    let current = root
    let pathSoFar = ''

    for (let i = 0; i < segments.length; i++) {
      const seg = segments[i]
      pathSoFar = pathSoFar ? `${pathSoFar}:${seg}` : seg
      const isLast = i === segments.length - 1

      if (!current.has(seg)) {
        current.set(seg, isLast ? null : new Map())
      }

      if (isLast) {
        // 叶子节点：如果之前是分支（有子节点），保留分支但也标记自身为 key
        const existing = current.get(seg)
        if (existing instanceof Map) {
          existing.set('__self__', { key: entry.key, ttlMillis: entry.ttlMillis })
        } else {
          current.set(seg, { __leaf__: true, key: entry.key, ttlMillis: entry.ttlMillis })
        }
      } else {
        const existing = current.get(seg)
        if (!(existing instanceof Map)) {
          current.set(seg, new Map())
        }
      }
      if (!isLast) {
        current = current.get(seg)
      }
    }
  }

  return mapToTreeNodes(root, '')
}

function mapToTreeNodes(map: Map<string, any>, parentPath: string): TreeNode[] {
  const nodes: TreeNode[] = []

  for (const [seg, value] of map) {
    if (seg === '__self__') continue
    const fullPath = parentPath ? `${parentPath}:${seg}` : seg

    if (value instanceof Map) {
      // 分支节点
      const selfEntry = value.get('__self__')
      const children = mapToTreeNodes(value, fullPath)
      const childCount = countLeaves(children)

      const node: TreeNode = {
        key: `dir:${fullPath}`,
        label: seg,
        isLeaf: false,
        prefixPath: fullPath + ':',
        childCount,
        children
      }

      // 如果分支本身也是一个 key（如 user:1 既是分支又是 key）
      if (selfEntry) {
        children.unshift({
          key: `leaf:${selfEntry.key}`,
          label: '(self)',
          isLeaf: true,
          fullKey: selfEntry.key,
          ttlMillis: selfEntry.ttlMillis
        })
        node.childCount = countLeaves(children)
      }

      nodes.push(node)
    } else if (value && value.__leaf__) {
      // 叶子节点
      nodes.push({
        key: `leaf:${value.key}`,
        label: seg,
        isLeaf: true,
        fullKey: value.key,
        ttlMillis: value.ttlMillis
      })
    }
  }

  return nodes.sort((a, b) => {
    // 分支在前，叶子在后
    if (a.isLeaf !== b.isLeaf) return a.isLeaf ? 1 : -1
    return a.label.localeCompare(b.label)
  })
}

function countLeaves(nodes: TreeNode[]): number {
  let count = 0
  for (const n of nodes) {
    if (n.isLeaf) count++
    else if (n.children) count += countLeaves(n.children)
  }
  return count
}

// ==================== 数据加载 ====================
async function fetchKeys() {
  loading.value = true
  try {
    const result = await getCacheKeysApi({
      prefix: prefix.value.trim() || undefined,
      limit: limit.value
    })
    keyEntries.value = result.keys
    keysTotal.value = result.total
    truncated.value = result.truncated
  } catch (e: any) {
    message.error(e.message || '加载 Keys 失败')
  } finally {
    loading.value = false
  }
}

// ==================== 树交互 ====================
function handleTreeSelect(keys: string[], options: TreeOption[]) {
  if (keys.length === 0) return
  const node = options[0] as TreeNode
  if (node.isLeaf && node.fullKey) {
    openValueModal(node.fullKey)
  }
}

function renderLabel({ option }: { option: TreeOption }) {
  const node = option as TreeNode
  if (node.isLeaf) {
    return h('span', { class: 'tree-leaf-label mono' }, [
      node.label,
      node.ttlMillis !== undefined && node.ttlMillis > 0
        ? h(NTag, { size: 'tiny', type: 'info', bordered: false, style: 'margin-left:6px' }, { default: () => formatTtl(node.ttlMillis!) })
        : null
    ])
  }
  return h('span', { class: 'tree-dir-label' }, [
    h('span', { style: 'font-weight:500' }, node.label),
    h('span', { class: 'tree-dir-count' }, ` (${node.childCount ?? 0})`)
  ])
}

function renderSuffix({ option }: { option: TreeOption }) {
  const node = option as TreeNode
  if (!node.isLeaf && node.prefixPath) {
    return h(NSpace, { size: 4, align: 'center', style: 'display:inline-flex' }, () => [
      h(
        NButton,
        {
          size: 'tiny',
          quaternary: true,
          type: 'primary',
          onClick: (e: MouseEvent) => {
            e.stopPropagation()
            openCreateModalWithPrefix(node.prefixPath!)
          }
        },
        { default: () => '+' }
      ),
      h(
        NPopconfirm,
        {
          onPositiveClick: () => handleDeletePrefix(node.prefixPath!),
          onClick: (e: MouseEvent) => e.stopPropagation()
        },
        {
          trigger: () =>
            h(
              NButton,
              {
                size: 'tiny',
                quaternary: true,
                type: 'error',
                onClick: (e: MouseEvent) => e.stopPropagation()
              },
              { default: () => '×' }
            ),
          default: () => `确定删除「${node.prefixPath}」下的全部 Key 吗？`
        }
      )
    ])
  }
  if (node.isLeaf && node.fullKey) {
    return h(
      NPopconfirm,
      {
        onPositiveClick: () => handleDeleteKey(node.fullKey!),
        onClick: (e: MouseEvent) => e.stopPropagation()
      },
      {
        trigger: () =>
          h(
            NButton,
            {
              size: 'tiny',
              quaternary: true,
              type: 'error',
              onClick: (e: MouseEvent) => e.stopPropagation()
            },
            { default: () => '删除' }
          ),
        default: () => `确定删除「${node.fullKey}」吗？`
      }
    )
  }
  return undefined
}

// ==================== 值查看/编辑 ====================
async function openValueModal(key: string) {
  showValueModal.value = true
  valueLoading.value = true
  currentKey.value = key
  currentValueInfo.value = null
  valueText.value = ''
  try {
    const info = await getCacheValueApi(key)
    currentValueInfo.value = info
    valueMode.value = info.type || 'text'
    valueText.value = formatValueToText(info.value)
  } catch (e: any) {
    if (e?.status === 404) {
      // 列表停留期间 key 已过期/被删：关闭弹窗并刷新，不把正常过期当错误弹红
      showValueModal.value = false
      message.warning('Key 不存在或已过期，已刷新列表')
      await fetchKeys()
    } else {
      message.error(e.message || '加载值失败')
    }
  } finally {
    valueLoading.value = false
  }
}

function formatValueToText(value: any): string {
  if (value === null || value === undefined) return ''
  if (typeof value === 'object') {
    try {
      return JSON.stringify(value, null, 2)
    } catch {
      return String(value)
    }
  }
  return String(value)
}

function handleFormatValue() {
  try {
    const parsed = JSON.parse(valueText.value)
    valueText.value = JSON.stringify(parsed, null, 2)
  } catch {
    message.warning('当前内容不是合法 JSON')
  }
}

async function handleSaveCurrentValue() {
  if (!currentKey.value) return
  if (!valueText.value.trim()) {
    message.warning('值不能为空（如需删除请使用删除按钮）')
    return
  }
  if (valueMode.value === 'json') {
    try {
      JSON.parse(valueText.value)
    } catch {
      message.error('JSON 格式不合法，请修正后保存')
      return
    }
  }
  savingValue.value = true
  try {
    await saveCacheValueApi(currentKey.value, valueText.value, TTL_NEVER_EXPIRES, valueMode.value)
    message.success('保存成功')
    await openValueModal(currentKey.value)
    await fetchKeys()
  } catch (e: any) {
    message.error(e.message || '保存失败')
  } finally {
    savingValue.value = false
  }
}

// ==================== 删除 ====================
async function handleDeleteKey(key: string) {
  try {
    await deleteCacheKeyApi(key)
    message.success('删除成功')
    if (currentKey.value === key) {
      showValueModal.value = false
    }
    await fetchKeys()
  } catch (e: any) {
    message.error(e.message || '删除失败')
  }
}

async function handleDeletePrefix(prefixPath: string) {
  try {
    const result = await deleteCachePrefixApi(prefixPath)
    message.success(`已删除 ${result.deleted} 个 Key${result.failed > 0 ? `，${result.failed} 个失败` : ''}`)
    await fetchKeys()
  } catch (e: any) {
    message.error(e.message || '删除失败')
  }
}

// ==================== 新增 ====================
function openCreateModal() {
  newKey.value = ''
  newValue.value = ''
  newTtl.value = TTL_NEVER_EXPIRES
  newMode.value = 'text'
  showCreateModal.value = true
}

function openCreateModalWithPrefix(prefixPath: string) {
  newKey.value = prefixPath
  newValue.value = ''
  newTtl.value = TTL_NEVER_EXPIRES
  newMode.value = 'text'
  showCreateModal.value = true
}

async function handleCreateKey() {
  const key = newKey.value.trim()
  if (!key) {
    message.warning('请输入 Key')
    return
  }
  if (!newValue.value.trim()) {
    message.warning('请输入 Value')
    return
  }
  if (newMode.value === 'json' && newValue.value.trim()) {
    try {
      JSON.parse(newValue.value)
    } catch {
      message.error('JSON 格式不合法，请修正后提交')
      return
    }
  }
  creatingKey.value = true
  try {
    await saveCacheValueApi(key, newValue.value, newTtl.value, newMode.value)
    message.success('新增成功')
    showCreateModal.value = false
    await fetchKeys()
  } catch (e: any) {
    message.error(e.message || '新增失败')
  } finally {
    creatingKey.value = false
  }
}

onMounted(() => {
  fetchKeys()
})
</script>

<template>
  <div class="cache-page flex h-full flex-col">
    <NCard :bordered="false" class="flex-1 rounded-lg" :content-style="{ padding: '16px', display: 'flex', flexDirection: 'column', height: '100%' }">
      <!-- 工具栏 -->
      <div class="toolbar">
        <NSpace align="center" :wrap="false">
          <NInput
            v-model:value="prefix"
            placeholder="按 Key 前缀过滤（如 user:）"
            clearable
            size="small"
            style="width: 320px"
            @keyup.enter="fetchKeys"
          />
          <NButton type="primary" size="small" :loading="loading" @click="fetchKeys">
            搜索
          </NButton>
          <NInputNumber
            v-model:value="limit"
            size="small"
            :min="1"
            :max="10000"
            :step="500"
            style="width: 140px"
            placeholder="返回数量"
          />
        </NSpace>
        <NSpace align="center">
          <NButton type="primary" size="small" @click="openCreateModal">
            新增
          </NButton>
          <NButton size="small" :loading="loading" @click="fetchKeys">
            刷新
          </NButton>
        </NSpace>
      </div>

      <!-- 统计 -->
      <div class="summary">
        共 {{ keysTotal }} 个 Key
        <NTag v-if="truncated" size="tiny" type="warning" :bordered="false" style="margin-left: 8px">
          已截断，请缩小前缀或增大 limit
        </NTag>
      </div>

      <!-- 树形列表 -->
      <div class="tree-wrapper">
        <NSpin :show="loading">
          <NEmpty
            v-if="!loading && keyEntries.length === 0"
            :description="prefix.trim() ? '未找到匹配的 Key' : '暂无缓存数据'"
            :style="{ padding: '60px 0' }"
          />
          <NTree
            v-else
            :data="treeData"
            block-line
            expand-on-click
            selectable
            virtual-scroll
            :style="{ height: '100%', maxHeight: '600px' }"
            :render-label="renderLabel"
            :render-suffix="renderSuffix"
            @update:selected-keys="handleTreeSelect"
          />
        </NSpin>
      </div>
    </NCard>

    <!-- 值详情弹窗 -->
    <WwModal
      v-model:show="showValueModal"
      title="缓存值详情"
      :width="720"
      :resizable="true"
    >
      <NSpin :show="valueLoading">
        <div v-if="currentValueInfo" class="value-detail">
          <div class="value-meta">
            <div class="meta-row">
              <span class="meta-label">Key</span>
              <span class="meta-value mono">{{ currentValueInfo.key }}</span>
            </div>
            <div class="meta-row">
              <span class="meta-label">类型</span>
              <NTag size="small" :type="valueMode === 'json' ? 'success' : 'default'" :bordered="false">
                {{ valueMode === 'json' ? 'JSON' : '文本' }}
              </NTag>
            </div>
          </div>

          <div class="value-edit-label">
            <NSpace align="center" justify="space-between" style="width:100%">
              <span>Value</span>
              <NRadioGroup v-model:value="valueMode" size="small">
                <NRadio value="json">JSON</NRadio>
                <NRadio value="text">文本</NRadio>
              </NRadioGroup>
            </NSpace>
          </div>

          <JsonEditor
            ref="valueEditorRef"
            v-model="valueText"
            :mode="valueMode"
            height="300px"
            placeholder="（空值）"
          />

          <div class="value-actions">
            <NSpace justify="end">
              <NButton v-if="valueMode === 'json'" @click="handleFormatValue">格式化</NButton>
              <NButton
                type="primary"
                :loading="savingValue"
                @click="handleSaveCurrentValue"
              >
                保存（永不过期）
              </NButton>
              <NPopconfirm @positive-click="handleDeleteKey(currentKey)">
                <template #trigger>
                  <NButton type="error">删除 Key</NButton>
                </template>
                确定删除 Key「{{ currentKey }}」吗？
              </NPopconfirm>
            </NSpace>
          </div>
        </div>
      </NSpin>
    </WwModal>

    <!-- 新增弹窗 -->
    <WwModal
      v-model:show="showCreateModal"
      title="新增 Key"
      :width="640"
    >
      <NForm label-placement="top" :show-feedback="false">
        <NFormItem label="Key">
          <NInput
            v-model:value="newKey"
            placeholder="如 user:1:profile"
          />
        </NFormItem>
        <NFormItem label="值类型">
          <NRadioGroup v-model:value="newMode" size="small">
            <NRadio value="text">文本</NRadio>
            <NRadio value="json">JSON</NRadio>
          </NRadioGroup>
        </NFormItem>
        <NFormItem label="Value">
          <JsonEditor
            ref="newEditorRef"
            v-model="newValue"
            :mode="newMode"
            height="200px"
            placeholder="请输入值"
          />
        </NFormItem>
        <NFormItem label="过期时间 TTL（毫秒，0 表示永不过期）">
          <NInputNumber
            v-model:value="newTtl"
            :min="0"
            :step="1000"
            style="width: 100%"
            placeholder="0 表示永不过期"
          />
        </NFormItem>
      </NForm>
      <template #footer>
        <NSpace justify="end">
          <NButton @click="showCreateModal = false">取消</NButton>
          <NButton
            type="primary"
            :loading="creatingKey"
            @click="handleCreateKey"
          >
            确定
          </NButton>
        </NSpace>
      </template>
    </WwModal>
  </div>
</template>

<style scoped>
.toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  flex-wrap: wrap;
  gap: 8px;
  margin-bottom: 12px;
}

.summary {
  font-size: 12px;
  color: var(--ww-text-3);
  padding: 0 0 10px;
  display: flex;
  align-items: center;
}

.tree-wrapper {
  flex: 1;
  min-height: 200px;
  overflow: auto;
}

.mono {
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
}

.tree-leaf-label {
  font-size: 13px;
}

.tree-dir-label {
  font-size: 13px;
}

.tree-dir-count {
  color: var(--ww-text-3);
  font-size: 12px;
}

.value-detail {
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.value-meta {
  background-color: var(--ww-surface-2);
  border: 1px solid var(--ww-border);
  border-radius: var(--ww-radius);
  padding: 10px 12px;
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.meta-row {
  display: flex;
  gap: 8px;
  font-size: 12px;
  align-items: center;
}

.meta-label {
  color: var(--ww-text-3);
  min-width: 48px;
}

.meta-value {
  word-break: break-all;
}

.value-edit-label {
  font-size: 12px;
  color: var(--ww-text-2);
  margin-top: 2px;
}

.value-actions {
  display: flex;
  justify-content: flex-end;
  padding-top: 8px;
}
</style>
