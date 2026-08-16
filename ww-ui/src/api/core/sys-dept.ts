import { requestClient } from '#/api/request';

export interface SysDept {
  id: number;
  deptName: string;
  parentId: number;
  ancestors?: string;
  orderNum: number;
  status: number;
  createdDate?: string;
}

/**
 * 获取部门列表（树形扁平）
 */
export async function getDeptListApi() {
  return requestClient.get<SysDept[]>('/dept/list');
}

/**
 * 获取部门详情
 */
export async function getDeptApi(id: number) {
  return requestClient.get<SysDept>(`/dept/${id}`);
}

/**
 * 保存部门（新增/更新）
 */
export async function saveDeptApi(data: Partial<SysDept>) {
  return requestClient.post<number>('/dept/save', data);
}

/**
 * 删除部门
 */
export async function deleteDeptApi(id: number) {
  return requestClient.delete(`/dept/${id}`);
}
