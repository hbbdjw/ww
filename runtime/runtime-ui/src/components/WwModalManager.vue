<template>
  <Teleport to="body">
    <div v-if="minimizedList.length > 0" class="ww-modal-manager">
      <div
        v-for="item in minimizedList"
        :key="item.id"
        class="ww-modal-minimized-btn"
        :title="item.title"
        @click="handleRestore(item)"
      >
        <svg :width="16" :height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <rect x="3" y="3" width="18" height="18" rx="2" />
          <path d="M9 12h6" />
        </svg>
        <span class="ww-modal-minimized-title">{{ item.title }}</span>
      </div>
    </div>
  </Teleport>
</template>

<script setup lang="ts">
import { useMinimizedRegistry } from '../composables/useMinimizedRegistry'

const { minimizedList } = useMinimizedRegistry()

function handleRestore(item: { id: string; restore: () => void }) {
  item.restore()
}
</script>

<style scoped>
.ww-modal-manager {
  position: fixed;
  right: 16px;
  bottom: 16px;
  display: flex;
  flex-direction: column;
  gap: 8px;
  z-index: 9999;
  pointer-events: none;
}

.ww-modal-minimized-btn {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 12px;
  background: var(--ww-surface-modal);
  border: 1px solid var(--ww-border);
  border-radius: var(--ww-radius);
  box-shadow: var(--ww-shadow-float);
  color: var(--ww-text-1);
  cursor: pointer;
  pointer-events: auto;
  transition: background 0.2s ease, border-color 0.2s ease, transform 0.2s ease, box-shadow 0.2s ease;
  max-width: 200px;
}

.ww-modal-minimized-btn:hover {
  background: var(--ww-surface-2);
  border-color: var(--ww-primary);
  box-shadow: var(--ww-shadow-popover);
  transform: translateX(-4px);
}

.ww-modal-minimized-btn svg {
  color: var(--ww-primary);
  flex-shrink: 0;
}

.ww-modal-minimized-title {
  font-size: 13px;
  color: var(--ww-text-1);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  flex: 1;
  min-width: 0;
}
</style>
