import { requestClient } from '#/api/request';

export interface SysRole {
  id: number;
  roleName: string;
  roleKey: string;
  dataScope: string;
  status: number;
  menus?: number[];
  menuIds?: number[];
  deptIds?: number[];
  createdDate?: string;
}

/**
 * 获取角色列表
 */
export async function getRoleListApi() {
  return requestClient.get<SysRole[]>('/role/list');
}

/**
 * 获取角色详情
 */
export async function getRoleApi(id: number) {
  return requestClient.get<SysRole>(`/role/${id}`);
}

/**
 * 获取角色已分配的菜单ID列表
 */
export async function getRoleMenuIdsApi(id: number) {
  return requestClient.get<number[]>(`/role/${id}/menuIds`);
}

/**
 * 获取角色自定义数据范围的部门ID列表
 */
export async function getRoleDeptIdsApi(id: number) {
  return requestClient.get<number[]>(`/role/${id}/deptIds`);
}

/**
 * 保存角色（新增/更新）
 */
export async function saveRoleApi(data: Partial<SysRole>) {
  return requestClient.post<number>('/role/save', data);
}

/**
 * 删除角色
 */
export async function deleteRoleApi(id: number) {
  return requestClient.delete(`/role/${id}`);
}

/**
 * 为角色分配菜单权限
 */
export async function assignMenusApi(roleId: number, menuIds: number[]) {
  return requestClient.post('/role/assignMenus', { roleId, menuIds });
}

/**
 * 为角色分配数据范围部门
 */
export async function assignDeptsApi(roleId: number, deptIds: number[]) {
  return requestClient.post('/role/assignDepts', { roleId, deptIds });
}
