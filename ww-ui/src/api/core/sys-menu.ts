import { requestClient } from '#/api/request';

export interface SysMenu {
  id: number;
  menuName: string;
  parentId: number;
  orderNum: number;
  path: string;
  component: string;
  menuType: string; // M=目录, C=菜单, F=按钮
  perms: string;
  status: number;
  createdDate?: string;
}

/**
 * 获取菜单列表
 */
export async function getMenuListApi() {
  return requestClient.get<SysMenu[]>('/menu/list');
}

/**
 * 获取菜单详情
 */
export async function getMenuApi(id: number) {
  return requestClient.get<SysMenu>(`/menu/${id}`);
}

/**
 * 保存菜单（新增/更新）
 */
export async function saveMenuApi(data: Partial<SysMenu>) {
  return requestClient.post<number>('/menu/save', data);
}

/**
 * 删除菜单
 */
export async function deleteMenuApi(id: number) {
  return requestClient.delete(`/menu/${id}`);
}
