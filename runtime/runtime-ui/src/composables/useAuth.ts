import { computed, ref } from 'vue'

/** 登录用户基础信息。 */
export interface AuthUser {
  userId: number
  username: string
  nickname: string
  deptId: number
}

const TOKEN_KEY = 'ww-runtime-token'

// 模块级单例状态：token 持久化于 localStorage，用户/权限信息会话内有效
const token = ref<string>(localStorage.getItem(TOKEN_KEY) || '')
const user = ref<AuthUser | null>(null)
const roles = ref<string[]>([])
const permissions = ref<string[]>([])
/** /api/auth/info 是否已加载（避免重复拉取）。 */
const infoLoaded = ref(false)

const isLoggedIn = computed(() => !!token.value)

function setToken(value: string) {
  token.value = value
  localStorage.setItem(TOKEN_KEY, value)
}

function setInfo(info: {
  userId: number
  username: string
  nickname: string
  deptId: number
  roles: string[]
  permissions: string[]
}) {
  user.value = {
    userId: info.userId,
    username: info.username,
    nickname: info.nickname,
    deptId: info.deptId
  }
  roles.value = info.roles || []
  permissions.value = info.permissions || []
  infoLoaded.value = true
}

function clearAuth() {
  token.value = ''
  user.value = null
  roles.value = []
  permissions.value = []
  infoLoaded.value = false
  localStorage.removeItem(TOKEN_KEY)
}

/** 是否拥有指定权限标识。 */
function hasPermission(perm: string): boolean {
  return permissions.value.includes(perm)
}

export function useAuth() {
  return {
    token,
    user,
    roles,
    permissions,
    infoLoaded,
    isLoggedIn,
    setToken,
    setInfo,
    clearAuth,
    hasPermission
  }
}
