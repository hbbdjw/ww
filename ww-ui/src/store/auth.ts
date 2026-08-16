import type { Recordable, UserInfo } from '@vben/types';

import { ref } from 'vue';
import { useRouter } from 'vue-router';

import { LOGIN_PATH } from '@vben/constants';
import { preferences, preferencesManager } from '@vben/preferences';
import { resetAllStores, useAccessStore, useUserStore } from '@vben/stores';

import { defineStore } from 'pinia';
import { useDebounceFn } from '@vueuse/core';

import { notification } from '#/adapter/naive';
import { getAccessCodesApi, getUserInfoApi, loginApi, logoutApi } from '#/api';
import { saveUserPreferencesApi } from '#/api';
import { $t } from '#/locales';

export const useAuthStore = defineStore('auth', () => {
  const accessStore = useAccessStore();
  const userStore = useUserStore();
  const router = useRouter();

  const loginLoading = ref(false);

  /**
   * 异步处理登录操作
   * Asynchronously handle the login process
   * @param params 登录表单数据
   */
  async function authLogin(
    params: Recordable<any>,
    onSuccess?: () => Promise<void> | void,
  ) {
    // 异步处理用户登录操作并获取 accessToken
    let userInfo: null | UserInfo = null;
    try {
      loginLoading.value = true;
      const { accessToken } = await loginApi(params);

      // 如果成功获取到 accessToken
      if (accessToken) {
        // 将 accessToken 存储到 accessStore 中
        accessStore.setAccessToken(accessToken);

        // 获取用户信息并存储到 accessStore 中
        const [fetchUserInfoResult, accessCodes] = await Promise.all([
          fetchUserInfo(),
          getAccessCodesApi(),
        ]);

        userInfo = fetchUserInfoResult;

        userStore.setUserInfo(userInfo);
        accessStore.setAccessCodes(accessCodes);

        if (accessStore.loginExpired) {
          accessStore.setLoginExpired(false);
        } else {
          onSuccess
            ? await onSuccess?.()
            : await router.push(
                userInfo.homePath || preferences.app.defaultHomePath,
              );
        }

        if (userInfo?.realName) {
          notification.success({
            content: $t('authentication.loginSuccess'),
            description: `${$t('authentication.loginSuccessDesc')}:${userInfo?.realName}`,
            duration: 3000,
          });
        }
      }
    } finally {
      loginLoading.value = false;
    }

    return {
      userInfo,
    };
  }

  async function logout(redirect: boolean = true) {
    // 注销偏好远程同步，避免登出后的偏好变更回写后端
    preferencesManager.setRemoteSync(null);
    try {
      await logoutApi();
    } catch {
      // 不做任何处理
    }
    resetAllStores();
    accessStore.setLoginExpired(false);

    // 回登录页带上当前路由地址
    await router.replace({
      path: LOGIN_PATH,
      query: redirect
        ? {
            redirect: encodeURIComponent(router.currentRoute.value.fullPath),
          }
        : {},
    });
  }

  async function fetchUserInfo() {
    const userInfo = await getUserInfoApi();
    userStore.setUserInfo(userInfo);

    // 应用后端持久化的用户偏好设置
    // 后端返回 preferences 为 JSON 字符串或 null（未自定义时）
    if (userInfo?.preferences) {
      try {
        const serverPreferences =
          typeof userInfo.preferences === 'string'
            ? JSON.parse(userInfo.preferences)
            : userInfo.preferences;
        // 用后端偏好覆盖当前状态
        preferencesManager.updatePreferences(serverPreferences);
      } catch {
        // 偏好 JSON 解析失败时忽略，使用默认值
      }
    }

    // 注册远程同步：用户修改偏好后 debounce 1s 回写后端
    // 避免频繁请求；fire-and-forget，错误静默处理
    const debouncedSave = useDebounceFn(async (json: string) => {
      try {
        await saveUserPreferencesApi(json);
      } catch {
        // 保存失败静默处理，不影响用户操作
      }
    }, 1000);
    preferencesManager.setRemoteSync(debouncedSave);

    return userInfo;
  }

  function $reset() {
    loginLoading.value = false;
  }

  return {
    $reset,
    authLogin,
    fetchUserInfo,
    loginLoading,
    logout,
  };
});
