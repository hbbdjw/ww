<script setup lang="ts">
import type { editor } from 'monaco-editor';

import { onBeforeUnmount, onMounted, ref, watch } from 'vue';

import monaco from './monaco-setup';

defineOptions({ name: 'CodeEditor' });

const props = withDefaults(defineProps<Props>(), {
  height: '100%',
  language: 'json',
  modelValue: '',
  readOnly: false,
  theme: 'vs',
});

const emit = defineEmits<{
  'update:modelValue': [value: string];
}>();

interface Props {
  /** Editor height (CSS value), fills parent by default */
  height?: string;
  /** Language (json / javascript / typescript / plaintext ...) */
  language?: string;
  /** Text content with two-way binding */
  modelValue?: string;
  /** Read only */
  readOnly?: boolean;
  /** Theme */
  theme?: 'vs' | 'vs-dark';
}

const containerRef = ref<HTMLDivElement>();
let monacoEditor: editor.IStandaloneCodeEditor | null = null;
/** Suppress change event loop when syncing value from outside */
let ignoreChange = false;

onMounted(() => {
  if (!containerRef.value) return;
  monacoEditor = monaco.editor.create(containerRef.value, {
    automaticLayout: true,
    fontSize: 13,
    language: props.language,
    minimap: { enabled: false },
    padding: { bottom: 8, top: 8 },
    readOnly: props.readOnly,
    renderWhitespace: 'none',
    scrollBeyondLastLine: false,
    scrollbar: { horizontalScrollbarSize: 8, verticalScrollbarSize: 8 },
    tabSize: 2,
    theme: props.theme,
    value: props.modelValue,
    wordWrap: 'on',
  });
  monacoEditor.onDidChangeModelContent(() => {
    if (ignoreChange || !monacoEditor) return;
    emit('update:modelValue', monacoEditor.getValue());
  });
});

watch(
  () => props.modelValue,
  (value) => {
    if (!monacoEditor || monacoEditor.getValue() === (value ?? '')) return;
    ignoreChange = true;
    monacoEditor.setValue(value ?? '');
    ignoreChange = false;
  },
);

watch(
  () => props.language,
  (language) => {
    const model = monacoEditor?.getModel();
    if (model) monaco.editor.setModelLanguage(model, language);
  },
);

watch(
  () => props.readOnly,
  (readOnly) => {
    monacoEditor?.updateOptions({ readOnly });
  },
);

watch(
  () => props.theme,
  (theme) => {
    monaco.editor.setTheme(theme);
  },
);

onBeforeUnmount(() => {
  monacoEditor?.dispose();
  monacoEditor = null;
});

/** Run document formatting (validates + formats JSON) */
function format() {
  return monacoEditor?.getAction('editor.action.formatDocument')?.run();
}

defineExpose({ format, getEditor: () => monacoEditor });
</script>

<template>
  <div
    ref="containerRef"
    :style="{ height: props.height }"
    class="code-editor"
  ></div>
</template>

<style scoped>
.code-editor {
  width: 100%;
  min-height: 120px;
  overflow: hidden;
  border: 1px solid var(--n-border-color, #e0e0e6);
  border-radius: 6px;
}
</style>
