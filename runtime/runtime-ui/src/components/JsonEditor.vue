<script setup lang="ts">
import { computed } from 'vue'
import { Codemirror } from 'vue-codemirror'
import { basicSetup } from 'codemirror'
import { json, jsonParseLinter } from '@codemirror/lang-json'
import { linter } from '@codemirror/lint'

/**
 * CodeMirror 6 封装：JSON 模式带语法高亮与实时校验（lint），文本模式纯编辑。
 */
const props = withDefaults(
  defineProps<{
    modelValue: string
    mode?: 'json' | 'text'
    readonly?: boolean
    height?: string
    placeholder?: string
  }>(),
  {
    mode: 'text',
    readonly: false,
    height: '320px',
    placeholder: '请输入值'
  }
)

const emit = defineEmits<{
  (e: 'update:modelValue', value: string): void
}>()

const extensions = computed(() => {
  const exts: any[] = [basicSetup]
  if (props.mode === 'json') {
    exts.push(json())
    exts.push(linter(jsonParseLinter()))
  }
  return exts
})

/** JSON 模式下校验当前值是否合法；文本模式恒为 true。 */
function validate(): boolean {
  if (props.mode !== 'json') return true
  try {
    JSON.parse(props.modelValue)
    return true
  } catch {
    return false
  }
}

defineExpose({ validate })
</script>

<template>
  <div class="json-editor" :style="{ height: props.height }">
    <Codemirror
      :model-value="modelValue"
      :extensions="extensions"
      :disabled="readonly"
      :placeholder="placeholder"
      :style="{ height: '100%', fontSize: '13px' }"
      @update:model-value="(v: string) => emit('update:modelValue', v)"
    />
  </div>
</template>

<style scoped>
.json-editor {
  /* 必须显式宽度：在 NFormItem 的 flex 内容区中，flex item 默认 shrink-to-fit，
     未设宽度时空编辑器会收缩成很窄一条 */
  width: 100%;
  border: 1px solid var(--ww-border);
  border-radius: var(--ww-radius);
  overflow: hidden;
  background: var(--ww-surface-2);
  transition: border-color 0.18s ease, box-shadow 0.18s ease;
}

.json-editor:focus-within {
  border-color: var(--ww-primary);
  box-shadow: 0 0 0 2px var(--ww-primary-soft);
}

.json-editor :deep(.cm-editor) {
  height: 100%;
  background: transparent;
}

.json-editor :deep(.cm-scroller) {
  font-family: ui-monospace, 'JetBrains Mono', SFMono-Regular, Menlo, Consolas, monospace;
}

.json-editor :deep(.cm-gutters) {
  background: transparent;
  border-right: 1px solid var(--ww-border);
}
</style>
