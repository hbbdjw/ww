import { ref, computed } from 'vue'

const BASE_Z_INDEX = 2000
const Z_INDEX_STEP = 10

interface ModalInstance {
  id: string
  zIndex: number
}

const modalStack = ref<ModalInstance[]>([])

let nextId = 0

export function useModalZIndex() {
  const generateId = () => {
    return `modal-${++nextId}`
  }

  const register = (id: string) => {
    const maxZ = modalStack.value.length > 0
      ? Math.max(...modalStack.value.map(m => m.zIndex))
      : BASE_Z_INDEX - Z_INDEX_STEP
    modalStack.value.push({ id, zIndex: maxZ + Z_INDEX_STEP })
  }

  const unregister = (id: string) => {
    const idx = modalStack.value.findIndex(m => m.id === id)
    if (idx !== -1) {
      modalStack.value.splice(idx, 1)
    }
  }

  const bringToFront = (id: string) => {
    const instance = modalStack.value.find(m => m.id === id)
    if (!instance) return BASE_Z_INDEX

    const maxZ = Math.max(...modalStack.value.map(m => m.zIndex))
    if (instance.zIndex < maxZ) {
      instance.zIndex = maxZ + Z_INDEX_STEP
    }
    return instance.zIndex
  }

  const getZIndex = (id: string) => {
    return modalStack.value.find(m => m.id === id)?.zIndex ?? BASE_Z_INDEX
  }

  return {
    generateId,
    register,
    unregister,
    bringToFront,
    getZIndex,
    modalStack: computed(() => modalStack.value)
  }
}
