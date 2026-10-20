import { get, post } from './request'
import type { AuthUser } from '@/composables/useAuth'

interface LoginResponse {
  token: string
  user: AuthUser
}

interface AuthInfo extends AuthUser {
  roles: string[]
  permissions: string[]
}

/** 登录，返回 token 与用户信息。 */
export async function loginApi(username: string, password: string): Promise<LoginResponse> {
  return post<LoginResponse>('/api/auth/login', { username, password })
}

/** 登出，撤销当前会话。 */
export async function logoutApi() {
  return post('/api/auth/logout')
}

/** 查询当前登录用户信息（角色 + 权限标识）。 */
export async function getAuthInfoApi(): Promise<AuthInfo> {
  return get<AuthInfo>('/api/auth/info')
}
