<script setup lang="ts">
import { ref } from 'vue';

import { NAlert, NButton, NCard, NSelect, NSpace, NSwitch } from 'naive-ui';

import { WwModal } from '#/components/WwModal';

/** 基础用法 */
const basicShow = ref(false);

/** 最大化 / 最小化 */
const windowShow = ref(false);

/** 遮罩与 zIndex */
const maskShow = ref(false);
const maskEnabled = ref(false);
const customZIndexShow = ref(false);

/** 多弹窗共存 */
const multiShow1 = ref(false);
const multiShow2 = ref(false);
const multiShow3 = ref(false);

/** 无遮罩多弹窗共存 */
const noMaskMultiShow1 = ref(false);
const noMaskMultiShow2 = ref(false);
const noMaskMultiShow3 = ref(false);

/** iframe 外链 */
const iframeShow = ref(false);

/** 自定义动画 */
const animationShow = ref(false);
const transitionName = ref('ww-modal-zoom');
const transitionOptions = [
  { label: '缩放（默认）', value: 'ww-modal-zoom' },
  { label: '淡入淡出', value: 'ww-modal-fade' },
  { label: '自下向上滑入', value: 'ww-modal-slide-up' },
  { label: '自上向下滑入', value: 'ww-modal-slide-down' },
];

/** 关闭拦截 */
const interceptShow = ref(false);
function onBeforeClose() {
  return window.confirm('确定要关闭该弹窗吗？');
}
</script>

<template>
  <div class="p-4">
    <NAlert class="mb-4" title="WwModal 通用弹出层使用示例" type="info">
      支持拖拽、四边四角缩放、最大/最小化、遮罩开关、zIndex
      设置、外链 iframe、多弹窗共存与层级自动切换、自定义弹出/关闭动画等能力。
    </NAlert>

    <div class="grid grid-cols-1 gap-4 lg:grid-cols-2">
      <NCard title="基础用法" size="small">
        <p class="mb-3 text-sm text-gray-500">
          标题、描述、底部确认/取消按钮，默认支持拖拽与缩放。
        </p>
        <NButton type="primary" @click="basicShow = true">打开弹窗</NButton>
      </NCard>

      <NCard title="最大化 / 最小化" size="small">
        <p class="mb-3 text-sm text-gray-500">
          头部双击可切换最大化；点击最小化按钮收纳到视口底部，点击底部标题条还原。
        </p>
        <NButton type="primary" @click="windowShow = true">打开弹窗</NButton>
      </NCard>

      <NCard title="遮罩开关 / 自定义 zIndex" size="small">
        <p class="mb-3 text-sm text-gray-500">
          通过 mask 控制遮罩显隐，zIndex 设置弹窗基础层级。
        </p>
        <NSpace align="center">
          <span class="text-sm">显示遮罩：</span>
          <NSwitch v-model:value="maskEnabled" />
          <NButton type="primary" @click="maskShow = true">无遮罩弹窗</NButton>
          <NButton @click="customZIndexShow = true">高 zIndex 弹窗</NButton>
        </NSpace>
      </NCard>

      <NCard title="多弹窗共存 / 层级自动切换" size="small">
        <p class="mb-3 text-sm text-gray-500">
          同时打开多个弹窗，点击任意弹窗会自动将其提升到最顶层；ESC 仅关闭最顶层弹窗。
        </p>
        <NSpace>
          <NButton @click="multiShow1 = true">弹窗一</NButton>
          <NButton @click="multiShow2 = true">弹窗二</NButton>
          <NButton @click="multiShow3 = true">弹窗三</NButton>
        </NSpace>
      </NCard>

      <NCard title="无遮罩多弹窗共存" size="small">
        <p class="mb-3 text-sm text-gray-500">
          多个无遮罩弹窗同时存在，不阻挡页面其他区域操作；点击任意弹窗自动置顶，可自由拖动错开摆放。
        </p>
        <NSpace>
          <NButton @click="noMaskMultiShow1 = true; noMaskMultiShow2 = true; noMaskMultiShow3 = true">
            一键打开三个弹窗
          </NButton>
          <NButton @click="noMaskMultiShow1 = true">弹窗 A</NButton>
          <NButton @click="noMaskMultiShow2 = true">弹窗 B</NButton>
          <NButton @click="noMaskMultiShow3 = true">弹窗 C</NButton>
        </NSpace>
      </NCard>

      <NCard title="外链 iframe" size="small">
        <p class="mb-3 text-sm text-gray-500">
          传入 iframeSrc 即可在弹窗中打开外链页面，点击 iframe 内部也会自动置顶。
        </p>
        <NButton type="primary" @click="iframeShow = true">打开外链</NButton>
      </NCard>

      <NCard title="自定义弹出/关闭动画" size="small">
        <p class="mb-3 text-sm text-gray-500">
          通过 transitionName 切换内置动画，也可传入自定义过渡类名；duration 控制时长。
        </p>
        <NSpace>
          <NSelect
            v-model:value="transitionName"
            :options="transitionOptions"
            class="w-45"
          />
          <NButton type="primary" @click="animationShow = true">打开弹窗</NButton>
        </NSpace>
      </NCard>

      <NCard title="关闭拦截" size="small">
        <p class="mb-3 text-sm text-gray-500">
          onBeforeClose 返回 false 时阻止关闭，可用于表单未保存提示等场景。
        </p>
        <NButton type="primary" @click="interceptShow = true">打开弹窗</NButton>
      </NCard>
    </div>

    <!-- 基础用法 -->
    <WwModal
      v-model:show="basicShow"
      title="基础弹窗"
      description="支持拖拽移动与四边四角缩放"
      @confirm="basicShow = false"
    >
      <p>弹窗内容：默认支持头部拖拽、边框与四角缩放。</p>
      <p class="mt-2">按 ESC 或点击遮罩可关闭。</p>
    </WwModal>

    <!-- 最大化 / 最小化 -->
    <WwModal
      v-model:show="windowShow"
      title="窗口化弹窗"
      width="720"
      height="480"
      minimizable
      @confirm="windowShow = false"
    >
      <ul class="list-disc pl-5 text-sm">
        <li>双击头部标题栏可切换最大化/还原</li>
        <li>点击右上角最小化按钮，弹窗会收纳到视口右下角</li>
        <li>最小化后点击底部标题条即可还原</li>
      </ul>
    </WwModal>

    <!-- 遮罩开关 -->
    <WwModal
      v-model:show="maskShow"
      title="无遮罩弹窗"
      :mask="maskEnabled"
      :mask-closable="maskEnabled"
      @confirm="maskShow = false"
    >
      <p>当前遮罩状态：{{ maskEnabled ? '开启' : '关闭' }}</p>
      <p class="mt-2 text-sm text-gray-500">
        无遮罩时仍可正常操作页面其他区域，适合工具类弹窗。
      </p>
    </WwModal>

    <!-- 自定义 zIndex -->
    <WwModal
      v-model:show="customZIndexShow"
      title="自定义层级弹窗"
      :z-index="5000"
      @confirm="customZIndexShow = false"
    >
      <p>该弹窗基础层级为 5000，始终显示在默认层级（2000 起）的弹窗之上。</p>
    </WwModal>

    <!-- 多弹窗共存 -->
    <WwModal
      v-model:show="multiShow1"
      title="弹窗一"
      width="560"
      height="360"
      minimizable
      @confirm="multiShow1 = false"
    >
      <p>我是弹窗一，点击我我会被提升到最顶层。</p>
      <NButton class="mt-4" @click="multiShow2 = true">
        在弹窗一上打开弹窗二
      </NButton>
    </WwModal>
    <WwModal
      v-model:show="multiShow2"
      title="弹窗二"
      width="480"
      height="320"
      minimizable
      @confirm="multiShow2 = false"
    >
      <p>我是弹窗二。</p>
      <NButton class="mt-4" @click="multiShow3 = true">打开弹窗三</NButton>
    </WwModal>
    <WwModal
      v-model:show="multiShow3"
      title="弹窗三"
      width="400"
      height="280"
      minimizable
      @confirm="multiShow3 = false"
    >
      <p>我是弹窗三，最后打开的弹窗默认在最上层。</p>
    </WwModal>

    <!-- 无遮罩多弹窗共存 -->
    <WwModal
      v-model:show="noMaskMultiShow1"
      title="无遮罩弹窗 A"
      width="420"
      :mask="false"
      minimizable
      @confirm="noMaskMultiShow1 = false"
    >
      <p>我没有遮罩，页面其他区域仍可正常点击操作。</p>
      <NButton class="mt-4" @click="noMaskMultiShow2 = true">打开弹窗 B</NButton>
    </WwModal>
    <WwModal
      v-model:show="noMaskMultiShow2"
      title="无遮罩弹窗 B"
      width="420"
      :mask="false"
      :offset-x="60"
      :offset-y="40"
      minimizable
      @confirm="noMaskMultiShow2 = false"
    >
      <p>点击我，我会自动切换到最顶层。</p>
      <NButton class="mt-4" @click="noMaskMultiShow3 = true">打开弹窗 C</NButton>
    </WwModal>
    <WwModal
      v-model:show="noMaskMultiShow3"
      title="无遮罩弹窗 C"
      width="420"
      :mask="false"
      :offset-x="120"
      :offset-y="80"
      minimizable
      @confirm="noMaskMultiShow3 = false"
    >
      <p>通过 offsetX/offsetY 级联错开打开，可同时查看多个弹窗内容。</p>
    </WwModal>

    <!-- iframe 外链 -->
    <WwModal
      v-model:show="iframeShow"
      title="外链页面"
      width="80vw"
      height="80vh"
      :footer="false"
      iframe-src="https://www.example.com"
    />

    <!-- 自定义动画 -->
    <WwModal
      v-model:show="animationShow"
      title="自定义动画"
      :transition-name="transitionName"
      :duration="350"
      @confirm="animationShow = false"
    >
      <p>当前动画：{{ transitionName }}</p>
    </WwModal>

    <!-- 关闭拦截 -->
    <WwModal
      v-model:show="interceptShow"
      title="关闭拦截"
      :on-before-close="onBeforeClose"
      @confirm="interceptShow = false"
    >
      <p>关闭该弹窗前会弹出确认框，取消则阻止关闭。</p>
    </WwModal>
  </div>
</template>
