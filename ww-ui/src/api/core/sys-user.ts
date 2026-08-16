import { requestClient } from '#/api/request';

export interface SysDept {
  id: number;
  deptName: string;
  parentId: number;
  orderNum: number;
  status: number;
}

export interface SysRole {
  id: number;
  roleName: string;
  roleKey: string;
  dataScope: string;
  status: number;
}

export interface SysUser {
  id: number;
  username: string;
  nickname: string;
  status: number;
  dept?: SysDept;
  roles?: SysRole[];
  createdDate?: string;
  lastModifiedDate?: string;
}

export interface PageResult<T> {
  items: T[];
  total: number;
}

/**
 * 分页查询用户
 */
export async function getUserPageApi(params: {
  pageNumber: number;
  pageSize: number;
  username?: string;
}): Promise<PageResult<SysUser>> {
  const result = await requestClient.get<{
    content: SysUser[];
    total: number;
  }>('/user/page', { params });
  return {
    items: result.content,
    total: result.total,
  };
}

/**
 * 获取用户详情
 */
export async function getUserApi(id: number) {
  return requestClient.get<SysUser>(`/user/${id}`);
}

/**
 * 保存用户（新增/更新）
 */
export async function saveUserApi(data: Record<string, any>) {
  return requestClient.post<number>('/user/save', data);
}

/**
 * 删除用户
 */
export async function deleteUserApi(id: number) {
  return requestClient.delete(`/user/${id}`);
}

/**
 * 重置用户密码
 */
export async function resetPasswordApi(userId: number, password: string) {
  return requestClient.post('/user/resetPassword', { userId, password });
}

/**
 * 分配角色
 */
export async function assignRolesApi(userId: number, roleIds: number[]) {
  return requestClient.post('/user/assignRoles', { userId, roleIds });
}

/**
 * 获取角色列表
 */
export async function getRoleListApi() {
  return requestClient.get<SysRole[]>('/role/list');
}

/**
 * 获取部门列表
 */
export async function getDeptListApi() {
  return requestClient.get<SysDept[]>('/dept/list');
}
