import type { UserInfo } from '@vben/types';

import { requestClient } from '#/api/request';

/**
 * 获取当前登录用户信息（角色 + 权限）
 * 后端 /auth/info 返回 vben 兼容的 UserInfo 格式
 */
export async function getUserInfoApi() {
  return requestClient.get<UserInfo>('/auth/info');
}

/**
 * 保存当前登录用户的前端偏好设置
 * @param preferencesJson 偏好设置的 JSON 字符串
 */
export async function saveUserPreferencesApi(preferencesJson: string) {
  return requestClient.put('/auth/preferences', { preferences: preferencesJson });
}
