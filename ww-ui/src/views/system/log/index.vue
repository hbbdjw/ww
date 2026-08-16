<script setup lang="ts">
import { computed, h, nextTick, onBeforeUnmount, onMounted, reactive, ref, watch } from 'vue';

import { Eraser, RotateCw, Search } from '@vben/icons';

import {
  type DataTableColumns,
  NButton,
  NCard,
  NDataTable,
  NDatePicker,
  NIcon,
  NInput,
  NPopconfirm,
  NSelect,
  NSpace,
  NTabPane,
  NTabs,
  NTag,
} from 'naive-ui';

import { message } from '#/adapter/naive';
import { getClusterNodesApi } from '#/api/core/cluster';
import {
  deleteLogsByTimeRangeApi,
  getLogPageApi,
  LOG_LEVELS,
  startLogStreamApi,
  type LogItem,
  type LogStreamHandle,
} from '#/api/core/log';

defineOptions({ name: 'SystemLog' });

// ==================== 历史日志 ====================
const historyLoading = ref(false);
const historyList = ref<LogItem[]>([]);
const historyPage = ref(1);
const historyPageSize = ref(15);
const historyTotal = ref(0);

const historyFilter = reactive({
  level: null as null | string,
  node: null as null | string,
  keyword: '',
  timeRange: null as null | [number, number],
});

/** 时间段删除的独立时间段选择 */
const deleteRange = ref<null | [number, number]>(null);

const nodeOptions = ref<{ label: string; value: string }[]>([]);
const levelOptions = LOG_LEVELS.map((level) => ({ label: level, value: level }));

async function loadNodes() {
  try {
    const res = await getClusterNodesApi();
    nodeOptions.value = res.nodes.map((n) => ({
      label: n.nodeKey,
      value: n.nodeKey,
    }));
  } catch {
    nodeOptions.value = [];
  }
}

async function fetchHistory() {
  historyLoading.value = true;
  try {
    const [startTime, endTime] = historyFilter.timeRange ?? [undefined, undefined];
    const res = await getLogPageApi({
      pageNumber: historyPage.value,
      pageSize: historyPageSize.value,
      level: historyFilter.level ?? undefined,
      node: historyFilter.node ?? undefined,
      keyword: historyFilter.keyword || undefined,
      startTime,
      endTime,
    });
    historyList.value = res.content;
    historyTotal.value = res.total;
  } catch {
    historyList.value = [];
    historyTotal.value = 0;
  } finally {
    historyLoading.value = false;
  }
}

function searchHistory() {
  historyPage.value = 1;
  void fetchHistory();
}

function resetHistory() {
  historyFilter.level = null;
  historyFilter.node = null;
  historyFilter.keyword = '';
  historyFilter.timeRange = null;
  searchHistory();
}

function levelTagType(level: string) {
  switch (level) {
    case 'ERROR': {
      return 'error' as const;
    }
    case 'WARN': {
      return 'warning' as const;
    }
    case 'INFO': {
      return 'info' as const;
    }
    default: {
      return 'default' as const;
    }
  }
}

const historyColumns: DataTableColumns<LogItem> = [
  { title: '时间', key: 'createdDate', width: 170 },
  {
    title: '级别',
    key: 'level',
    width: 90,
    render: (row) =>
      h(
        NTag,
        { type: levelTagType(row.level), size: 'small', bordered: false },
        { default: () => row.level },
      ),
  },
  { title: '节点', key: 'node', width: 150, ellipsis: { tooltip: true } },
  {
    title: 'Logger',
    key: 'loggerName',
    width: 220,
    ellipsis: { tooltip: true },
  },
  {
    title: '消息',
    key: 'message',
    minWidth: 280,
    ellipsis: { tooltip: true },
    render: (row) => displayMessage(row.message),
  },
];

const historyPagination = computed(() => ({
  page: historyPage.value,
  pageSize: historyPageSize.value,
  itemCount: historyTotal.value,
  showSizePicker: true,
  pageSizes: [15, 20, 50, 100],
  onChange: (page: number) => {
    historyPage.value = page;
    void fetchHistory();
  },
  onUpdatePageSize: (pageSize: number) => {
    historyPageSize.value = pageSize;
    historyPage.value = 1;
    void fetchHistory();
  },
}));

async function handleDeleteByRange() {
  if (!deleteRange.value) {
    message.warning('请先选择要删除的时间段');
    return;
  }
  const [startTime, endTime] = deleteRange.value;
  const count = await deleteLogsByTimeRangeApi(startTime, endTime);
  message.success(`已删除 ${count} 条日志`);
  deleteRange.value = null;
  await fetchHistory();
}

// ==================== 实时日志 ====================
const realtimeRunning = ref(false);
const realtimePaused = ref(false);
const realtimeLogs = ref<LogItem[]>([]);
const realtimeFilter = reactive({
  level: null as null | string,
  node: null as null | string,
});

/**
 * 客户端关键字过滤（纯前端，不触发 SSE 重连）。
 * <p>绑定到输入框 v-model，实时随输入变化重新过滤显示；底层 {@link #realtimeLogs} 全量保留。
 */
const realtimeClientKeyword = ref('');

/**
 * 去除首尾空白的客户端关键字（避免纯空格输入导致全部过滤失效）。
 */
const trimmedClientKeyword = computed(() => {
  return (realtimeClientKeyword.value ?? '').trim();
});

/** 按客户端关键字过滤后的实时日志（渲染时使用；底层 realtimeLogs 全量保留）。 */
const filteredRealtimeLogs = computed(() => {
  const kw = trimmedClientKeyword.value;
  if (!kw) return realtimeLogs.value;
  const lower = kw.toLowerCase();
  return realtimeLogs.value.filter((log) => {
    if (log.message && log.message.toLowerCase().includes(lower)) return true;
    if (log.loggerName && log.loggerName.toLowerCase().includes(lower)) return true;
    if (log.node && log.node.toLowerCase().includes(lower)) return true;
    if (log.stackTrace && log.stackTrace.toLowerCase().includes(lower)) return true;
    return false;
  });
});

const MAX_REALTIME_LOGS = 1000;
const realtimeBox = ref<HTMLElement | null>(null);
let streamHandle: LogStreamHandle | null = null;

/**
 * 实时日志框动态高度（px）：根据元素当前顶部位置与视口可用高度计算，窗口 resize 时自动刷新。
 * 模板中以 {@code :style="{ height: `${realtimeBoxHeight}px` }"} 应用，替代硬编码的 h-[520px]。
 */
const realtimeBoxHeight = ref(520);
function calcRealtimeBoxHeight() {
  realtimeBoxHeight.value = window.innerHeight - 30 - 190;
}

let resizeHandler: (() => void) | null = null;

function startRealtime() {
  realtimeLogs.value = [];
  realtimeRunning.value = true;
  realtimePaused.value = false;
  streamHandle = startLogStreamApi(
    {
      level: realtimeFilter.level ?? undefined,
      node: realtimeFilter.node ?? undefined,
    },
    (log) => {
      if (realtimePaused.value) return;
      realtimeLogs.value.push(log);
      if (realtimeLogs.value.length > MAX_REALTIME_LOGS) {
        realtimeLogs.value.splice(
          0,
          realtimeLogs.value.length - MAX_REALTIME_LOGS,
        );
      }
    },
    () => {
      if (realtimeRunning.value) {
        realtimeRunning.value = false;
        streamHandle = null;
        message.error('实时日志连接已断开');
      }
    },
  );
}

function stopRealtime() {
  streamHandle?.stop();
  streamHandle = null;
  realtimeRunning.value = false;
  realtimePaused.value = false;
}

function toggleRealtime() {
  if (realtimeRunning.value) {
    stopRealtime();
  } else {
    startRealtime();
  }
}

function clearRealtime() {
  realtimeLogs.value = [];
}

function formatTime(ts?: number) {
  if (!ts) return '';
  const d = new Date(ts);
  const p = (n: number) => String(n).padStart(2, '0');
  const ms = String(d.getMilliseconds()).padStart(3, '0');
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}.${ms}`;
}

/** 消息体展示：去掉 p6spy 等消息内容自带的 "yyyy-MM-dd HH:mm:ss.SSS | " 时间前缀，避免与行首时间重复 */
const EMBEDDED_TIME_PREFIX = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} \| /;
function displayMessage(msg?: string): string {
  if (!msg) return '';
  return msg.replace(EMBEDDED_TIME_PREFIX, '');
}

watch(
  () => filteredRealtimeLogs.value.length,
  async () => {
    await nextTick();
    const box = realtimeBox.value;
    if (box) {
      box.scrollTop = box.scrollHeight;
    }
  },
);

onMounted(() => {
  void loadNodes();
  void fetchHistory();
  nextTick(() => calcRealtimeBoxHeight());
  resizeHandler = () => calcRealtimeBoxHeight();
  window.addEventListener('resize', resizeHandler);
});

onBeforeUnmount(() => {
  stopRealtime();
  if (resizeHandler) {
    window.removeEventListener('resize', resizeHandler);
    resizeHandler = null;
  }
});
</script>

<template>
  <div class="p-2 flex h-full flex-col">
    <NCard :bordered="false" class="flex-1 rounded-lg" content-style="padding-top:0;" headerStyle="font-size:18px;" >
      <NTabs type="line">
        <NTabPane name="history" tab="历史日志">
          <!-- 筛选工具栏 -->
          <NSpace class="mb-3" :wrap="true" :size="[8, 8]" align="center">
            <NSelect
              v-model:value="historyFilter.level"
              :options="levelOptions"
              placeholder="日志级别"
              clearable
              style="width: 130px"
            />
            <NSelect
              v-model:value="historyFilter.node"
              :options="nodeOptions"
              placeholder="集群节点"
              clearable
              filterable
              style="width: 160px"
            />
            <NInput
              v-model:value="historyFilter.keyword"
              placeholder="关键字"
              clearable
              style="width: 200px"
              @keyup.enter="searchHistory"
            />
            <NDatePicker
              v-model:value="historyFilter.timeRange"
              type="datetimerange"
              clearable
              style="width: 340px"
            />
            <NButton type="primary" @click="searchHistory">
              <template #icon>
                <NIcon><Search /></NIcon>
              </template>
              查询
            </NButton>
            <NButton @click="resetHistory">
              <template #icon>
                <NIcon><RotateCw /></NIcon>
              </template>
              重置
            </NButton>
            <div class="flex-1"></div>
            <NDatePicker
              v-model:value="deleteRange"
              type="datetimerange"
              clearable
              style="width: 340px"
            />
            <NPopconfirm @positive-click="handleDeleteByRange">
              <template #trigger>
                <NButton type="error">删除时间段日志</NButton>
              </template>
              确定删除所选时间段内的所有日志吗？此操作不可恢复。
            </NPopconfirm>
          </NSpace>

          <NDataTable
            :max-height="realtimeBoxHeight - 90"
            remote
            :columns="historyColumns"
            :data="historyList"
            :loading="historyLoading"
            :pagination="historyPagination"
            :scroll-x="1000"
            :row-key="(row: LogItem) => row.id ?? row.createdDate ?? row.timestamp ?? 0"
          />
        </NTabPane>

        <NTabPane name="realtime" tab="实时日志">
          <!-- 实时工具栏 -->
          <NSpace class="mb-3" :wrap="true" :size="[8, 8]" align="center">
            <NButton
              :type="realtimeRunning ? 'error' : 'primary'"
              @click="toggleRealtime"
            >
              {{ realtimeRunning ? '停止' : '开始' }}
            </NButton>
            <NButton
              :disabled="!realtimeRunning"
              @click="realtimePaused = !realtimePaused"
            >
              {{ realtimePaused ? '继续' : '暂停' }}
            </NButton>
            <NButton :disabled="realtimeRunning" @click="clearRealtime">
              <template #icon>
                <NIcon><Eraser /></NIcon>
              </template>
              清空
            </NButton>
            <NSelect
              v-model:value="realtimeFilter.level"
              :options="levelOptions"
              placeholder="级别过滤"
              clearable
              :disabled="realtimeRunning"
              style="width: 130px"
            />
            <NSelect
              v-model:value="realtimeFilter.node"
              :options="nodeOptions"
              placeholder="节点过滤"
              clearable
              filterable
              :disabled="realtimeRunning"
              style="width: 160px"
            />
            <NInput
              v-model:value="realtimeClientKeyword"
              placeholder="关键字过滤（客户端实时）"
              clearable
              style="width: 240px"
            />
          </NSpace>

          <div
            ref="realtimeBox"
            class="overflow-auto rounded border border-gray-200 bg-gray-50 p-2 font-mono text-xs dark:border-gray-700 dark:bg-gray-900"
            :style="{ height: `${realtimeBoxHeight}px` }"
          >
            <div
              v-if="!realtimeRunning && realtimeLogs.length === 0"
              class="flex h-full items-center justify-center text-gray-400"
            >
              点击「开始」订阅实时日志
            </div>
            <div
              v-for="(log, index) in filteredRealtimeLogs"
              :key="index"
              class="border-b border-gray-100 py-0.5 leading-5 dark:border-gray-800"
            >
              <!-- 全部使用行内元素：文本流换行时从容器行首开始，不会被元信息顶出缩进 -->
              <span class="mr-2 text-gray-400">{{ formatTime(log.timestamp) }}</span>
              <NTag :type="levelTagType(log.level)" size="small" :bordered="false" class="mr-2 align-middle">
                {{ log.level }}
              </NTag>
              <span class="mr-2 rounded bg-gray-200 px-1 text-gray-600 dark:bg-gray-700 dark:text-gray-300">
                {{ log.node }}
              </span>
              <span v-if="log.loggerName" class="mr-2 text-gray-500">{{ log.loggerName }}</span>
              <span v-if="log.threadName" class="mr-2 text-gray-400">[{{ log.threadName }}]</span>
              <span class="whitespace-pre-wrap break-all">{{ displayMessage(log.message) }}</span>
              <span
                v-if="log.stackTrace"
                class="block whitespace-pre-wrap break-all text-gray-400"
              >{{ log.stackTrace }}</span>
            </div>
          </div>
        </NTabPane>
      </NTabs>
    </NCard>
  </div>
</template>
