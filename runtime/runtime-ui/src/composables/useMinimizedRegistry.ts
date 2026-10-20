import { ref } from 'vue'

export interface MinimizedItem {
  id: string
  title: string
  restore: () => void
}

const minimizedList = ref<MinimizedItem[]>([])

export function registerMinimized(item: MinimizedItem) {
  if (!minimizedList.value.find(m => m.id === item.id)) {
    minimizedList.value.push(item)
  }
}

export function unregisterMinimized(id: string) {
  minimizedList.value = minimizedList.value.filter(m => m.id !== id)
}

export function useMinimizedRegistry() {
  return {
    minimizedList,
    registerMinimized,
    unregisterMinimized,
  }
}
