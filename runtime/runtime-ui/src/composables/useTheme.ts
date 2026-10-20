import { computed, ref, watch } from 'vue'
import type { GlobalThemeOverrides } from 'naive-ui'

export interface PrimaryPreset {
  id: string
  label: string
  color: string
  hover: string
  pressed: string
}

const PRIMARY_PRESETS: PrimaryPreset[] = [
  { id: 'green', label: '青绿（默认）', color: '#18a058', hover: '#36ad6a', pressed: '#0c7a43' },
  { id: 'blue', label: '科技蓝', color: '#2080f0', hover: '#4098fc', pressed: '#1060c9' },
  { id: 'purple', label: '梦幻紫', color: '#722ed1', hover: '#9254de', pressed: '#531dab' },
  { id: 'orange', label: '琥珀橙', color: '#f0a020', hover: '#fcb040', pressed: '#c97c10' },
  { id: 'rose', label: '玫红', color: '#d03050', hover: '#de576d', pressed: '#ab1f3f' }
]

const THEME_KEY = 'ww-runtime-theme'
const PRIMARY_KEY = 'ww-runtime-primary'

const isDark = ref(localStorage.getItem(THEME_KEY) === 'dark')
watch(
  isDark,
  (dark) => {
    document.documentElement.classList.toggle('dark', dark)
    localStorage.setItem(THEME_KEY, dark ? 'dark' : 'light')
  },
  { immediate: true }
)

const storedPrimary = localStorage.getItem(PRIMARY_KEY)
const primaryId = ref(
  PRIMARY_PRESETS.some((p) => p.id === storedPrimary) ? (storedPrimary as string) : 'green'
)
const primaryPreset = computed(
  () => PRIMARY_PRESETS.find((p) => p.id === primaryId.value) || PRIMARY_PRESETS[0]!
)
watch(primaryId, (id) => localStorage.setItem(PRIMARY_KEY, id))

const themeOverrides = computed<GlobalThemeOverrides>(() => ({
  common: {
    primaryColor: primaryPreset.value.color,
    primaryColorHover: primaryPreset.value.hover,
    primaryColorPressed: primaryPreset.value.pressed,
    primaryColorSuppl: primaryPreset.value.hover
  }
}))

// ------------------------------------------------------------------
// 将主色预设同步为全局 --ww-* CSS 变量，供非 naive 组件（WwModal、
// 自定义面板、CodeMirror 等 Teleport 场景）使用同一品牌色
// ------------------------------------------------------------------
function hexToRgb(hex: string): { r: number; g: number; b: number } | null {
  const m = /^#?([0-9a-f]{6})$/i.exec(hex.trim())
  if (!m) return null
  const v = parseInt(m[1]!, 16)
  return { r: (v >> 16) & 255, g: (v >> 8) & 255, b: v & 255 }
}

function syncPrimaryVars() {
  const root = document.documentElement
  const p = primaryPreset.value
  root.style.setProperty('--ww-primary', p.color)
  root.style.setProperty('--ww-primary-hover', p.hover)
  root.style.setProperty('--ww-primary-pressed', p.pressed)
  const rgb = hexToRgb(p.color)
  if (rgb) {
    const dark = root.classList.contains('dark')
    root.style.setProperty(
      '--ww-primary-soft',
      `rgba(${rgb.r}, ${rgb.g}, ${rgb.b}, ${dark ? 0.18 : 0.12})`
    )
    root.style.setProperty(
      '--ww-primary-soft-hover',
      `rgba(${rgb.r}, ${rgb.g}, ${rgb.b}, ${dark ? 0.28 : 0.2})`
    )
  }
}

watch(
  [isDark, primaryPreset],
  () => syncPrimaryVars(),
  { immediate: true }
)

export function useTheme() {
  return {
    isDark,
    primaryId,
    primaryPreset,
    PRIMARY_PRESETS,
    themeOverrides
  }
}
