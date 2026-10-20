<script setup lang="ts">
import {
  NConfigProvider, NMessageProvider, NDialogProvider, NNotificationProvider, darkTheme
} from 'naive-ui'
import { useTheme } from '@/composables/useTheme'
import WwModalManager from '@/components/WwModalManager.vue'

const { isDark, themeOverrides } = useTheme()
</script>

<template>
  <n-config-provider :theme="isDark ? darkTheme : undefined" :theme-overrides="themeOverrides">
    <n-dialog-provider>
      <n-notification-provider>
        <n-message-provider>
          <router-view />
        </n-message-provider>
      </n-notification-provider>
    </n-dialog-provider>
    <WwModalManager />
  </n-config-provider>
</template>
