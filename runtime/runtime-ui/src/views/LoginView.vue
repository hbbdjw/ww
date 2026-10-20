<script setup lang="ts">
import { ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { NButton, NCard, NForm, NFormItem, NInput, NSpace, useMessage } from 'naive-ui'
import IconLogin from '~icons/mdi/login'
import { getAuthInfoApi, loginApi } from '@/api/auth'
import { useAuth } from '@/composables/useAuth'
import { useTheme } from '@/composables/useTheme'

defineOptions({ name: 'LoginView' })

const router = useRouter()
const route = useRoute()
const message = useMessage()
const auth = useAuth()
const { primaryPreset } = useTheme()

const username = ref('')
const password = ref('')
const loading = ref(false)

async function handleLogin() {
  if (!username.value.trim()) {
    message.warning('请输入用户名')
    return
  }
  if (!password.value) {
    message.warning('请输入密码')
    return
  }
  loading.value = true
  try {
    const result = await loginApi(username.value.trim(), password.value)
    auth.setToken(result.token)
    const info = await getAuthInfoApi()
    auth.setInfo(info)

    message.success(`欢迎回来，${info.nickname}`)
    const redirect = (route.query.redirect as string) || '/'
    router.replace(redirect)
  } catch (e: any) {
    message.error(e.message || '登录失败')
  } finally {
    loading.value = false
  }
}
</script>

<template>
  <div class="login-page">
    <div class="login-bg">
      <div class="bg-circle circle-1"></div>
      <div class="bg-circle circle-2"></div>
      <div class="bg-circle circle-3"></div>
    </div>

    <NCard class="login-card" :bordered="false" size="large">
      <div class="login-header">
        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor" class="login-logo" :style="{ color: primaryPreset.color }">
          <path d="M4 4h16a2 2 0 012 2v12a2 2 0 01-2 2H4a2 2 0 01-2-2V6a2 2 0 012-2zm0 2v12h16V6H4zm2 2h4v4H6V8zm6 0h6v2h-6V8zm0 4h6v2h-6v-2zm-6 2h4v2H6v-2z"/>
        </svg>
        <div class="login-title">WW Runtime</div>
        <div class="login-subtitle">分布式运行时管理控制台</div>
      </div>

      <NForm @submit.prevent="handleLogin">
        <NFormItem path="username">
          <NInput
            v-model:value="username"
            placeholder="请输入用户名"
            size="large"
            clearable
            @keyup.enter="handleLogin"
          >
          </NInput>
        </NFormItem>
        <NFormItem path="password">
          <NInput
            v-model:value="password"
            type="password"
            placeholder="请输入密码"
            size="large"
            show-password-on="click"
            @keyup.enter="handleLogin"
          />
        </NFormItem>

        <NButton
          type="primary"
          size="large"
          block
          :loading="loading"
          class="login-btn"
          @click="handleLogin"
        >
          登 录
        </NButton>

        <NSpace justify="center" class="login-tip">
          <span>默认账号：admin / admin123</span>
        </NSpace>
      </NForm>
    </NCard>
  </div>
</template>

<style scoped>
.login-page {
  position: relative;
  height: 100vh;
  display: flex;
  align-items: center;
  justify-content: center;
  overflow: hidden;
  background: linear-gradient(135deg, #0f2027 0%, #203a43 45%, #2c5364 100%);
}

.login-bg {
  position: absolute;
  inset: 0;
}

.bg-circle {
  position: absolute;
  border-radius: 50%;
  background: rgba(255, 255, 255, 0.06);
}

.circle-1 {
  width: 480px;
  height: 480px;
  top: -160px;
  left: -120px;
}

.circle-2 {
  width: 360px;
  height: 360px;
  bottom: -140px;
  right: -80px;
}

.circle-3 {
  width: 220px;
  height: 220px;
  top: 40%;
  left: 60%;
  background: rgba(255, 255, 255, 0.04);
}

.login-card {
  position: relative;
  z-index: 1;
  width: 380px;
  border-radius: 14px;
  box-shadow: 0 20px 60px rgba(0, 0, 0, 0.35);
}

.login-header {
  text-align: center;
  margin-bottom: 26px;
}

.login-logo {
  width: 52px;
  height: 52px;
}

.login-title {
  margin-top: 12px;
  font-size: 22px;
  font-weight: 700;
  letter-spacing: 0.5px;
  color: var(--ww-text-1);
}

.login-subtitle {
  margin-top: 6px;
  font-size: 13px;
  color: var(--ww-text-3);
}

.login-btn {
  margin-top: 6px;
  font-weight: 600;
  letter-spacing: 4px;
}

.login-tip {
  margin-top: 16px;
  font-size: 12px;
  color: var(--ww-text-3);
}
</style>
