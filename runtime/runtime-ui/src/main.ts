import { createApp } from 'vue'
import { createRouter, createWebHashHistory } from 'vue-router'
import naive from 'naive-ui'
import App from './App.vue'
import './styles/global.css'

import AdminLayout from './layouts/AdminLayout.vue'
import LoginView from './views/LoginView.vue'
import CacheManage from './views/CacheManage.vue'
import ClusterManage from './views/ClusterManage.vue'
import LogManage from './views/LogManage.vue'
import StorageManage from './views/StorageManage.vue'
import UserManage from './views/system/UserManage.vue'
import DeptManage from './views/system/DeptManage.vue'
import RoleManage from './views/system/RoleManage.vue'
import MonitorDashboard from './views/system/MonitorDashboard.vue'
import ModalTest from './views/ModalTest.vue'

import { useAuth } from './composables/useAuth'
import { getAuthInfoApi } from './api/auth'

const routes = [
  { path: '/login', component: LoginView, meta: { title: '登录' } },
  {
    path: '/',
    component: AdminLayout,
    children: [
      { path: '', redirect: '/system/cluster' },
      { path: 'system/users', component: UserManage, meta: { title: '用户管理' } },
      { path: 'system/depts', component: DeptManage, meta: { title: '部门管理' } },
      { path: 'system/roles', component: RoleManage, meta: { title: '角色管理' } },
      { path: 'system/cluster', component: ClusterManage, meta: { title: '集群管理' } },
      { path: 'system/cache', component: CacheManage, meta: { title: '缓存管理' } },
      { path: 'system/storage', component: StorageManage, meta: { title: '存储管理' } },
      { path: 'system/logs', component: LogManage, meta: { title: '日志管理' } },
      { path: 'system/monitor', component: MonitorDashboard, meta: { title: '运维监控' } },
      { path: 'test/modal', component: ModalTest, meta: { title: '弹窗测试' } }
    ]
  },
  { path: '/:pathMatch(.*)*', redirect: '/system/cluster' }
]

const router = createRouter({
  history: createWebHashHistory(),
  routes
})

// 全局守卫：未登录跳登录页（携带 redirect）；已登录但未加载权限信息则补拉
router.beforeEach(async (to) => {
  const auth = useAuth()

  if (to.path === '/login') {
    return auth.isLoggedIn.value ? '/' : true
  }

  if (!auth.token.value) {
    return to.fullPath !== '/'
      ? { path: '/login', query: { redirect: to.fullPath } }
      : { path: '/login' }
  }

  if (!auth.infoLoaded.value) {
    try {
      auth.setInfo(await getAuthInfoApi())
    } catch {
      auth.clearAuth()
      return { path: '/login' }
    }
  }
  return true
})

const app = createApp(App)
app.use(naive)
app.use(router)
app.mount('#app')
