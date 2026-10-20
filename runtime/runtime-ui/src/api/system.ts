import { del, get, post, put } from './request'

// ------------------------------------------------------------------
// 类型
// ------------------------------------------------------------------

export interface SysUser {
  userId: number
  deptId: number
  deptName: string | null
  username: string
  nickname: string
  email: string | null
  phone: string | null
  gender: number
  status: number
  loginAt: number | null
  remark: string | null
  roleIds: number[]
  roleNames: string[]
}

export interface SysUserPayload {
  username?: string
  nickname: string
  password?: string
  deptId: number
  email?: string
  phone?: string
  gender: number
  status: number
  remark?: string
  roleIds: number[]
}

export interface UserPage {
  total: number
  items: SysUser[]
}

export interface SysDept {
  deptId: number
  parentId: number
  deptName: string
  orderNum: number
  leader: string | null
  phone: string | null
  status: number
}

export interface SysRole {
  roleId: number
  roleName: string
  roleKey: string
  roleSort: number
  status: number
  dataScope: number
  remark: string | null
  menuIds: number[]
}

export interface SysMenuNode {
  menuId: number
  menuName: string
  menuType: string
  path: string | null
  perms: string | null
  icon: string | null
  orderNum: number
  children: SysMenuNode[]
}

// ------------------------------------------------------------------
// 用户
// ------------------------------------------------------------------

export function getUserPageApi(params: {
  keyword?: string
  deptId?: number
  page: number
  size: number
}): Promise<UserPage> {
  return get<UserPage>('/api/system/users', {
    keyword: params.keyword || undefined,
    deptId: params.deptId || undefined,
    page: params.page,
    size: params.size
  })
}

export function createUserApi(payload: SysUserPayload) {
  return post('/api/system/users', payload)
}

export function updateUserApi(userId: number, payload: SysUserPayload) {
  return put(`/api/system/users/${userId}`, payload)
}

export function deleteUserApi(userId: number) {
  return del(`/api/system/users/${userId}`)
}

export function resetPasswordApi(userId: number, password: string) {
  return put(`/api/system/users/${userId}/reset-password`, { password })
}

// ------------------------------------------------------------------
// 部门
// ------------------------------------------------------------------

export function listDeptsApi(): Promise<{ items: SysDept[] }> {
  return get('/api/system/depts')
}

export function createDeptApi(payload: Partial<SysDept>) {
  return post('/api/system/depts', payload)
}

export function updateDeptApi(deptId: number, payload: Partial<SysDept>) {
  return put(`/api/system/depts/${deptId}`, payload)
}

export function deleteDeptApi(deptId: number) {
  return del(`/api/system/depts/${deptId}`)
}

// ------------------------------------------------------------------
// 角色
// ------------------------------------------------------------------

export function listRolesApi(): Promise<{ items: SysRole[] }> {
  return get('/api/system/roles')
}

export function createRoleApi(payload: Partial<SysRole>) {
  return post('/api/system/roles', payload)
}

export function updateRoleApi(roleId: number, payload: Partial<SysRole>) {
  return put(`/api/system/roles/${roleId}`, payload)
}

export function deleteRoleApi(roleId: number) {
  return del(`/api/system/roles/${roleId}`)
}

export function assignRoleMenusApi(roleId: number, menuIds: number[]) {
  return put(`/api/system/roles/${roleId}/menus`, { menuIds })
}

// ------------------------------------------------------------------
// 菜单
// ------------------------------------------------------------------

export function menuTreeApi(): Promise<{ items: SysMenuNode[] }> {
  return get('/api/system/menus')
}
