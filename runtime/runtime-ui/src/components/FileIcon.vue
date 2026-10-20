<script setup lang="ts">
import { computed, type Component } from 'vue'
import { getFileCategory } from '@/api/storage'

// vscode-icons 构建时编译为 Vue 组件（完全离线，无运行时 API 请求）
import IconFolder from '~icons/vscode-icons/default-folder'
import IconFile from '~icons/vscode-icons/default-file'
import IconImage from '~icons/vscode-icons/file-type-image'
import IconSvg from '~icons/vscode-icons/file-type-svg'
import IconWebp from '~icons/vscode-icons/file-type-webp'
import IconVideo from '~icons/vscode-icons/file-type-video'
import IconAudio from '~icons/vscode-icons/file-type-audio'
import IconPdf from '~icons/vscode-icons/file-type-pdf2'
import IconZip from '~icons/vscode-icons/file-type-zip'
import IconJava from '~icons/vscode-icons/file-type-java'
import IconJs from '~icons/vscode-icons/file-type-js'
import IconTs from '~icons/vscode-icons/file-type-typescript'
import IconVue from '~icons/vscode-icons/file-type-vue'
import IconPython from '~icons/vscode-icons/file-type-python'
import IconGo from '~icons/vscode-icons/file-type-go'
import IconRust from '~icons/vscode-icons/file-type-rust'
import IconC from '~icons/vscode-icons/file-type-c'
import IconCpp from '~icons/vscode-icons/file-type-cpp'
import IconSql from '~icons/vscode-icons/file-type-sql'
import IconHtml from '~icons/vscode-icons/file-type-html'
import IconCss from '~icons/vscode-icons/file-type-css'
import IconShell from '~icons/vscode-icons/file-type-shell'
import IconBat from '~icons/vscode-icons/file-type-bat'
import IconGradle from '~icons/vscode-icons/file-type-gradle'
import IconMaven from '~icons/vscode-icons/file-type-maven'
import IconNode from '~icons/vscode-icons/file-type-node'
import IconMarkdown from '~icons/vscode-icons/file-type-markdown'
import IconJson from '~icons/vscode-icons/file-type-json'
import IconYaml from '~icons/vscode-icons/file-type-yaml'
import IconXml from '~icons/vscode-icons/file-type-xml'
import IconLog from '~icons/vscode-icons/file-type-log'
import IconConfig from '~icons/vscode-icons/file-type-config'
import IconIni from '~icons/vscode-icons/file-type-ini'
import IconToml from '~icons/vscode-icons/file-type-toml'
import IconWord from '~icons/vscode-icons/file-type-word'
import IconExcel from '~icons/vscode-icons/file-type-excel'
import IconPowerpoint from '~icons/vscode-icons/file-type-powerpoint'

/** 扩展名 → 专属图标（精确匹配优先） */
const EXT_ICONS: Record<string, Component> = {
  svg: IconSvg,
  webp: IconWebp,
  pdf: IconPdf,
  zip: IconZip,
  java: IconJava,
  js: IconJs,
  mjs: IconJs,
  cjs: IconJs,
  jsx: IconJs,
  ts: IconTs,
  tsx: IconTs,
  vue: IconVue,
  py: IconPython,
  go: IconGo,
  rs: IconRust,
  c: IconC,
  h: IconC,
  cpp: IconCpp,
  cc: IconCpp,
  hpp: IconCpp,
  sql: IconSql,
  html: IconHtml,
  htm: IconHtml,
  css: IconCss,
  scss: IconCss,
  less: IconCss,
  sh: IconShell,
  bat: IconBat,
  cmd: IconBat,
  ps1: IconBat,
  gradle: IconGradle,
  xml: IconXml,
  pom: IconMaven,
  json: IconJson,
  md: IconMarkdown,
  markdown: IconMarkdown,
  yaml: IconYaml,
  yml: IconYaml,
  log: IconLog,
  ini: IconIni,
  conf: IconConfig,
  cfg: IconConfig,
  env: IconConfig,
  toml: IconToml,
  doc: IconWord,
  docx: IconWord,
  xls: IconExcel,
  xlsx: IconExcel,
  csv: IconExcel,
  ppt: IconPowerpoint,
  pptx: IconPowerpoint,
  node: IconNode
}

/** 类别 → 图标（扩展名未命中时回退） */
const CATEGORY_ICONS: Record<string, Component> = {
  image: IconImage,
  video: IconVideo,
  audio: IconAudio,
  pdf: IconPdf,
  archive: IconZip,
  code: IconNode,
  text: IconLog
}

const props = withDefaults(
  defineProps<{
    /** 文件/目录名 */
    name: string
    /** MIME 类型（可选，辅助分类） */
    contentType?: string
    /** 渲染尺寸（px），默认 20 */
    size?: number
    /** 强制显示文件夹图标 */
    folder?: boolean
  }>(),
  { size: 20, folder: false }
)

const icon = computed<Component>(() => {
  if (props.folder) return IconFolder
  const cat = getFileCategory(props.name, props.contentType)
  const ext = props.name.toLowerCase().split('.').pop() || ''
  return EXT_ICONS[ext] ?? CATEGORY_ICONS[cat] ?? IconFile
})
</script>

<template>
  <component
    :is="icon"
    :style="{ width: `${size}px`, height: `${size}px`, flexShrink: 0 }"
    class="file-icon"
  />
</template>

<style scoped>
.file-icon {
  display: inline-block;
  vertical-align: middle;
}
</style>
