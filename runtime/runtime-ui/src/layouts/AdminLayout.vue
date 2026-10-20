<script setup lang="ts">
import { computed, h, onMounted, ref, watch } from 'vue'
import {
  NLayout, NLayoutHeader, NLayoutSider, NButton, NSpace, NDropdown, NDrawer, NDrawerContent,
  NRadioGroup, NRadio, NDivider, NTooltip, type MenuOption
} from 'naive-ui'
import { useRoute, useRouter, RouterLink } from 'vue-router'
import { getRuntimeStatus, type RuntimeStatus } from '@/api/cluster'
import { menuTreeApi, type SysMenuNode } from '@/api/system'
import { logoutApi } from '@/api/auth'
import { useAuth } from '@/composables/useAuth'
import { useTheme } from '@/composables/useTheme'

import IconSystem from '~icons/mdi/cog-outline'
import IconUsers from '~icons/mdi/account-multiple'
import IconDept from '~icons/mdi/sitemap-outline'
import IconRole from '~icons/mdi/shield-account-outline'
import IconCluster from '~icons/mdi/server-network'
import IconCache from '~icons/mdi/cached'
import IconStorage from '~icons/mdi/database'
import IconLogs from '~icons/mdi/text-box-multiple-outline'
import IconMonitor from '~icons/mdi/chart-line'
import IconCollapse from '~icons/mdi/chevron-double-left'
import IconExpand from '~icons/mdi/chevron-double-right'
import IconSun from '~icons/mdi/white-balance-sunny'
import IconMoon from '~icons/mdi/weather-night'
import IconCheck from '~icons/mdi/check'
import IconPalette from '~icons/mdi/palette'
import IconLogout from '~icons/mdi/logout-variant'

defineOptions({ name: 'AdminLayout' })

const route = useRoute()
const router = useRouter()
const auth = useAuth()
const status = ref<RuntimeStatus | null>(null)
const collapsed = ref(false)

const pageTitle = computed(() => (route.meta?.title as string) || 'WW Runtime')

// ------------------------------------------------------------------
// 菜单：从后端菜单树构建，按当前用户权限过滤（按钮节点不渲染）
// ------------------------------------------------------------------

const ICONS: Record<string, unknown> = {
  'mdi:cog-outline': IconSystem,
  'mdi:account-multiple': IconUsers,
  'mdi:sitemap-outline': IconDept,
  'mdi:shield-account-outline': IconRole,
  'mdi:server-network': IconCluster,
  'mdi:cached': IconCache,
  'mdi:database': IconStorage,
  'mdi:text-box-multiple-outline': IconLogs,
  'mdi:chart-line': IconMonitor
}

const menuOptions = ref<MenuOption[]>([])

function buildOptions(nodes: SysMenuNode[]): MenuOption[] {
  const result: MenuOption[] = []
  for (const node of nodes) {
    if (node.menuType === 'F') {
      continue
    }
    const children = node.children ? buildOptions(node.children) : []
    // 菜单节点需具备对应 list 权限；目录仅在存在可见子项时保留
    if (node.menuType === 'C' && node.perms && !auth.hasPermission(node.perms)) {
      continue
    }
    if (node.menuType === 'M' && children.length === 0) {
      continue
    }
    const icon = node.icon ? ICONS[node.icon] : undefined
    const path = node.path
    const option: MenuOption = {
      label: path
        ? () => h(RouterLink, { to: path }, { default: () => node.menuName })
        : node.menuName,
      key: path || `dir-${node.menuId}`,
      icon: icon ? () => h(icon) : undefined
    }
    if (children.length > 0) {
      option.children = children
    }
    result.push(option)
  }
  return result
}

async function loadMenus() {
  const tree = await menuTreeApi()
  menuOptions.value = buildOptions(tree.items)
}

// 全局主题与配色（共享 composable，App 的 NConfigProvider 与本页主题抽屉共用同一状态）
const { isDark, primaryId, primaryPreset, PRIMARY_PRESETS } = useTheme()

const themeDrawerVisible = ref(false)

// ------------------------------------------------------------------
// 用户下拉
// ------------------------------------------------------------------

const userOptions = computed(() => [
  {
    label: auth.user.value?.username || '',
    key: 'username',
    disabled: true
  },
  { type: 'divider', key: 'd1' },
  {
    label: '退出登录',
    key: 'logout',
    icon: () => h(IconLogout)
  }
])

async function handleUserAction(key: string) {
  if (key !== 'logout') {
    return
  }
  try {
    await logoutApi()
  } finally {
    auth.clearAuth()
    router.replace('/login')
  }
}

async function fetchStatus() {
  try {
    status.value = await getRuntimeStatus()
  } catch {
    // ignore
  }
}

onMounted(() => {
  loadMenus()
  fetchStatus()
})
</script>

<template>
  <n-layout has-sider style="height: 100vh;">
    <n-layout-sider
      collapse-mode="width"
      :collapsed-width="64"
      :width="220"
      :collapsed="collapsed"
      inverted
      content-style="display: flex; flex-direction: column;"
    >
      <div class="logo-container">
        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="white" style="width: 28px; height: 28px; flex-shrink: 0;">
          <path d="M4 4h16a2 2 0 012 2v12a2 2 0 01-2 2H4a2 2 0 01-2-2V6a2 2 0 012-2zm0 2v12h16V6H4zm2 2h4v4H6V8zm6 0h6v2h-6V8zm0 4h6v2h-6v-2zm-6 2h4v2H6v-2z"/>
        </svg>
        <span class="logo-text">WW Runtime</span>
      </div>
      <n-menu
        inverted
        :collapsed="collapsed"
        :collapsed-width="64"
        :collapsed-icon-size="20"
        :options="menuOptions"
        :value="route.path"
        :default-expanded-keys="['dir-1', '/system']"
        :indent="18"
        style="flex: 1; overflow-y: auto;"
      />
      <div class="sider-footer">
        <n-tooltip placement="right" :disabled="!collapsed">
          <template #trigger>
            <button class="collapse-btn" :title="collapsed ? '展开菜单' : '收起菜单'" @click="collapsed = !collapsed">
              <component :is="collapsed ? IconExpand : IconCollapse" width="18" height="18" />
            </button>
          </template>
          展开菜单
        </n-tooltip>
      </div>
    </n-layout-sider>

    <n-layout>
      <n-layout-header bordered class="app-header">
        <div class="page-title">{{ pageTitle }}</div>
        <n-space align="center" :size="8">
          <span v-if="status" class="header-node-info">
            当前节点: <strong class="mono">{{ status.nodeId }}</strong>
          </span>
          <n-button size="small" quaternary @click="fetchStatus">刷新</n-button>
          <n-tooltip trigger="hover">
            <template #trigger>
              <n-button size="small" quaternary class="theme-btn" @click="themeDrawerVisible = true">
                <template #icon><IconPalette /></template>
                主题
              </n-button>
            </template>
            全局主题设置
          </n-tooltip>
          <n-dropdown
            :options="userOptions"
            trigger="click"
            @select="handleUserAction"
          >
            <n-button size="small" quaternary>
              {{ auth.user?.nickname || auth.user?.username || '用户' }}
            </n-button>
          </n-dropdown>
        </n-space>
      </n-layout-header>
      <n-layout-content class="app-content" content-style="height: 100%; padding: 20px;" style="height: calc(100vh - 56px); overflow: auto;">
        <router-view v-slot="{ Component }">
          <!--
            关键点（修复「切菜单内容区空白、刷新才显示」）：
            1. 按 route.path 绑定唯一 key，确保切换菜单时目标组件被正确重建而非错误复用；
            2. 不使用 <transition mode="out-in"> 包裹——out-in 会先卸载旧组件、待其 leave
               结束后再挂载新组件；当目标为路由级异步组件时，解析/挂载时序一旦错位，
               就出现「旧节点已移除、新节点未插入」的空白窗。纯 CSS 入场动画不介入
               挂载时序，既保留淡入效果，又杜绝空白。
          -->
          <div :key="route.path" class="route-pane">
            <component :is="Component" />
          </div>
        </router-view>
      </n-layout-content>
    </n-layout>

    <n-drawer v-model:show="themeDrawerVisible" :width="340" placement="right">
      <n-drawer-content title="主题设置" closable :native-scrollbar="false">
        <div class="text-xs ww-text-3 mb-3">外观</div>
        <n-radio-group :value="isDark ? 'dark' : 'light'" class="theme-options" @update:value="(v: 'light' | 'dark') => isDark = v === 'dark'">
          <n-radio value="light" class="theme-option">
            <n-space align="center" :size="10">
              <IconSun width="18" height="18" class="text-amber-400" />
              <span>浅色模式</span>
            </n-space>
          </n-radio>
          <n-radio value="dark" class="theme-option">
            <n-space align="center" :size="10">
              <IconMoon width="18" height="18" class="text-indigo-400" />
              <span>深色模式</span>
            </n-space>
          </n-radio>
        </n-radio-group>
        <n-divider class="!my-4" />
        <div class="text-xs ww-text-3 mb-3">全局配色</div>
        <div class="preset-grid">
          <button
            v-for="p in PRIMARY_PRESETS"
            :key="p.id"
            class="preset-chip"
            :class="{ active: primaryId === p.id }"
            :style="{ '--preset-color': p.color }"
            :title="p.label"
            @click="primaryId = p.id"
          >
            <IconCheck v-if="primaryId === p.id" width="16" height="16" color="#fff" />
          </button>
        </div>
        <div class="mt-2 text-xs ww-text-2">当前：{{ primaryPreset.label }}</div>
        <n-divider class="!my-4" />
        <div class="text-xs ww-text-3">主题偏好将保存在浏览器本地，下次访问自动应用。</div>
      </n-drawer-content>
    </n-drawer>
  </n-layout>
</template>

<style>
.logo-container {
  display: flex;
  align-items: center;
  gap: 10px;
  height: 56px;
  padding: 0 18px;
  flex-shrink: 0;
  overflow: hidden;
  border-bottom: 1px solid rgba(255, 255, 255, 0.08);
}

.logo-text {
  color: #fff;
  font-size: 16px;
  font-weight: 600;
  letter-spacing: 0.2px;
  white-space: nowrap;
}

.app-header {
  height: 56px;
  padding: 0 20px;
  display: flex;
  align-items: center;
  justify-content: space-between;
}

.page-title {
  font-weight: 600;
  font-size: 15px;
  line-height: 22px;
  color: var(--ww-text-1);
}

.header-node-info {
  font-size: 12px;
  color: var(--ww-text-3);
}

.header-node-info strong {
  color: var(--ww-text-2);
  font-weight: 600;
}

.app-content {
  background: var(--ww-bg);
}

.sider-footer {
  flex-shrink: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 12px 0;
  border-top: 1px solid rgba(255, 255, 255, 0.08);
}

.collapse-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 34px;
  height: 34px;
  border: none;
  border-radius: 50%;
  color: rgba(255, 255, 255, 0.65);
  background: rgba(255, 255, 255, 0.08);
  cursor: pointer;
  transition: all 0.2s ease;
}

.collapse-btn:hover {
  color: #fff;
  background: var(--ww-primary);
}

.theme-options {
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.theme-options .n-radio {
  margin: 0;
}

.preset-grid {
  display: flex;
  flex-wrap: wrap;
  gap: 14px;
}

.preset-chip {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 32px;
  height: 32px;
  padding: 0;
  border: none;
  border-radius: 50%;
  background: var(--preset-color);
  cursor: pointer;
  box-shadow: inset 0 0 0 1px rgba(0, 0, 0, 0.08);
  transition: transform 0.18s ease;
}

.preset-chip:hover {
  transform: scale(1.12);
}

.preset-chip.active {
  box-shadow: 0 0 0 2px var(--ww-surface), 0 0 0 4px var(--preset-color);
}
</style>
