<script setup lang="ts">
import { ref } from 'vue'
import { NButton, NCard, NSpace, NInput, NInputNumber, NRadioGroup, NRadio, NDivider } from 'naive-ui'
import WwModal from '@/components/WwModal.vue'

defineOptions({ name: 'ModalTest' })

const showBasic = ref(false)
const showNoMask = ref(false)
const showResizable = ref(false)
const showMaximizable = ref(false)
const showMinimizable = ref(false)
const showDraggable = ref(false)
const showIframe = ref(false)
const showCustomButtons = ref(false)
const showCustomStyle = ref(false)
const showPosition1 = ref(false)
const showPosition2 = ref(false)
const showPosition3 = ref(false)
const showPosition4 = ref(false)
const showPosition5 = ref(false)
const showPosition6 = ref(false)
const showPosition7 = ref(false)
const showPosition8 = ref(false)
const showPosition9 = ref(false)
const showMulti1 = ref(false)
const showMulti2 = ref(false)
const showMulti3 = ref(false)
const showComprehensive = ref(false)

const form = ref({
  name: '',
  age: 0,
  type: 'text'
})

const extraButtons = [
  {
    key: 'refresh',
    icon: 'refresh',
    label: '刷新',
    onClick: () => alert('刷新按钮被点击')
  },
  {
    key: 'download',
    icon: 'download',
    label: '下载',
    onClick: () => alert('下载按钮被点击')
  }
]
</script>

<template>
  <div class="modal-test-page">
    <h1 class="ww-text-1" style="margin-bottom: 20px; font-size: 20px; font-weight: 600;">WwModal 组件测试页面</h1>

    <NSpace vertical :size="20">
      <!-- 基础示例 -->
      <NCard title="基础弹窗">
        <NButton type="primary" @click="showBasic = true">打开基础弹窗</NButton>
        <WwModal
          v-model:show="showBasic"
          title="基础弹窗"
          :width="500"
        >
          <div>这是一个基础的弹窗示例</div>
          <NInput v-model:value="form.name" placeholder="输入测试" />
          <template #footer>
            <NButton @click="showBasic = false">关闭</NButton>
          </template>
        </WwModal>
      </NCard>

      <!-- 无遮罩 -->
      <NCard title="无遮罩弹窗">
        <NButton type="primary" @click="showNoMask = true">打开无遮罩弹窗</NButton>
        <WwModal
          v-model:show="showNoMask"
          title="无遮罩弹窗"
          :width="400"
          :mask="false"
        >
          <div>这个弹窗没有遮罩层，可以与背景内容交互</div>
          <template #footer>
            <NButton @click="showNoMask = false">关闭</NButton>
          </template>
        </WwModal>
      </NCard>

      <!-- 可缩放 -->
      <NCard title="可缩放弹窗">
        <NButton type="primary" @click="showResizable = true">打开可缩放弹窗</NButton>
        <WwModal
          v-model:show="showResizable"
          title="可缩放弹窗"
          :width="500"
          :height="300"
          :resizable="true"
          :min-width="300"
          :min-height="200"
        >
          <div>拖动边框或角落可以调整弹窗大小</div>
          <template #footer>
            <NButton @click="showResizable = false">关闭</NButton>
          </template>
        </WwModal>
      </NCard>

      <!-- 可最大化 -->
      <NCard title="可最大化弹窗">
        <NButton type="primary" @click="showMaximizable = true">打开可最大化弹窗</NButton>
        <WwModal
          v-model:show="showMaximizable"
          title="可最大化弹窗"
          :width="500"
          :maximizable="true"
        >
          <div>点击右上角的最大化按钮或双击标题栏可以最大化弹窗</div>
          <template #footer>
            <NButton @click="showMaximizable = false">关闭</NButton>
          </template>
        </WwModal>
      </NCard>

      <!-- 可最小化 -->
      <NCard title="可最小化弹窗">
        <NButton type="primary" @click="showMinimizable = true">打开可最小化弹窗</NButton>
        <WwModal
          v-model:show="showMinimizable"
          title="可最小化弹窗"
          :width="500"
          :minimizable="true"
        >
          <div>点击最小化按钮后，弹窗会缩到右下角，点击悬浮按钮可以恢复</div>
          <template #footer>
            <NButton @click="showMinimizable = false">关闭</NButton>
          </template>
        </WwModal>
      </NCard>

      <!-- 可拖拽 -->
      <NCard title="可拖拽弹窗">
        <NButton type="primary" @click="showDraggable = true">打开可拖拽弹窗</NButton>
        <WwModal
          v-model:show="showDraggable"
          title="可拖拽弹窗"
          :width="500"
          :draggable="true"
        >
          <div>拖动标题栏可以移动弹窗位置</div>
          <template #footer>
            <NButton @click="showDraggable = false">关闭</NButton>
          </template>
        </WwModal>
      </NCard>

      <!-- iframe 弹窗 -->
      <NCard title="iframe 弹窗">
        <NButton type="primary" @click="showIframe = true">打开 iframe 弹窗</NButton>
        <WwModal
          v-model:show="showIframe"
          title="iframe 弹窗"
          :width="800"
          :height="500"
          iframe-src="https://www.example.com"
        />
      </NCard>

      <!-- 自定义按钮 -->
      <NCard title="自定义按钮弹窗">
        <NButton type="primary" @click="showCustomButtons = true">打开自定义按钮弹窗</NButton>
        <WwModal
          v-model:show="showCustomButtons"
          title="自定义按钮弹窗"
          :width="500"
          :extra-buttons="extraButtons"
        >
          <div>标题栏有自定义的刷新和下载按钮</div>
          <template #footer>
            <NButton @click="showCustomButtons = false">关闭</NButton>
          </template>
        </WwModal>
      </NCard>

      <!-- 自定义样式 -->
      <NCard title="自定义样式弹窗">
        <NButton type="primary" @click="showCustomStyle = true">打开自定义样式弹窗</NButton>
        <WwModal
          v-model:show="showCustomStyle"
          title="自定义样式弹窗"
          :width="500"
          padding="24px"
          title-font-size="20px"
          :header-style="{ background: 'var(--ww-surface-2)', borderBottom: '2px solid var(--ww-primary)' }"
          :body-style="{ background: 'var(--ww-surface-2)' }"
        >
          <div>这个弹窗有自定义的 padding、标题字体大小、头部和主体背景色</div>
          <template #footer>
            <NButton @click="showCustomStyle = false">关闭</NButton>
          </template>
        </WwModal>
      </NCard>

      <!-- 位置示例 -->
      <NCard title="弹窗位置">
        <NSpace>
          <NButton @click="showPosition1 = true">居中 (center)</NButton>
          <NButton @click="showPosition2 = true">左侧 (left)</NButton>
          <NButton @click="showPosition3 = true">右侧 (right)</NButton>
          <NButton @click="showPosition4 = true">顶部 (top)</NButton>
          <NButton @click="showPosition5 = true">底部 (bottom)</NButton>
          <NButton @click="showPosition6 = true">左上 (top-left)</NButton>
          <NButton @click="showPosition7 = true">右上 (top-right)</NButton>
          <NButton @click="showPosition8 = true">左下 (bottom-left)</NButton>
          <NButton @click="showPosition9 = true">右下 (bottom-right)</NButton>
        </NSpace>

        <WwModal v-model:show="showPosition1" title="居中" position="center" :width="300">
          <div>位置：center</div>
          <template #footer><NButton @click="showPosition1 = false">关闭</NButton></template>
        </WwModal>

        <WwModal v-model:show="showPosition2" title="左侧" position="left" :width="300">
          <div>位置：left</div>
          <template #footer><NButton @click="showPosition2 = false">关闭</NButton></template>
        </WwModal>

        <WwModal v-model:show="showPosition3" title="右侧" position="right" :width="300">
          <div>位置：right</div>
          <template #footer><NButton @click="showPosition3 = false">关闭</NButton></template>
        </WwModal>

        <WwModal v-model:show="showPosition4" title="顶部" position="top" :width="300">
          <div>位置：top</div>
          <template #footer><NButton @click="showPosition4 = false">关闭</NButton></template>
        </WwModal>

        <WwModal v-model:show="showPosition5" title="底部" position="bottom" :width="300">
          <div>位置：bottom</div>
          <template #footer><NButton @click="showPosition5 = false">关闭</NButton></template>
        </WwModal>

        <WwModal v-model:show="showPosition6" title="左上" position="top-left" :width="300">
          <div>位置：top-left</div>
          <template #footer><NButton @click="showPosition6 = false">关闭</NButton></template>
        </WwModal>

        <WwModal v-model:show="showPosition7" title="右上" position="top-right" :width="300">
          <div>位置：top-right</div>
          <template #footer><NButton @click="showPosition7 = false">关闭</NButton></template>
        </WwModal>

        <WwModal v-model:show="showPosition8" title="左下" position="bottom-left" :width="300">
          <div>位置：bottom-left</div>
          <template #footer><NButton @click="showPosition8 = false">关闭</NButton></template>
        </WwModal>

        <WwModal v-model:show="showPosition9" title="右下" position="bottom-right" :width="300">
          <div>位置：bottom-right</div>
          <template #footer><NButton @click="showPosition9 = false">关闭</NButton></template>
        </WwModal>
      </NCard>

      <!-- 多弹窗示例 -->
      <NCard title="多弹窗 z-index 管理">
        <NSpace>
          <NButton type="primary" @click="showMulti1 = true">打开弹窗 1</NButton>
          <NButton type="primary" @click="showMulti2 = true">打开弹窗 2</NButton>
          <NButton type="primary" @click="showMulti3 = true">打开弹窗 3</NButton>
        </NSpace>
        <p class="ww-text-2" style="margin-top: 10px; font-size: 13px;">
          提示：打开多个弹窗后，点击不同的弹窗，观察 z-index 自动切换
        </p>

        <WwModal v-model:show="showMulti1" title="弹窗 1" :width="400" position="top-left" :mask="false">
          <div>这是弹窗 1，点击我会置顶</div>
          <template #footer><NButton @click="showMulti1 = false">关闭</NButton></template>
        </WwModal>

        <WwModal v-model:show="showMulti2" title="弹窗 2" :width="400" position="center" :mask="false">
          <div>这是弹窗 2，点击我会置顶</div>
          <template #footer><NButton @click="showMulti2 = false">关闭</NButton></template>
        </WwModal>

        <WwModal v-model:show="showMulti3" title="弹窗 3" :width="400" position="bottom-right" :mask="false">
          <div>这是弹窗 3，点击我会置顶</div>
          <template #footer><NButton @click="showMulti3 = false">关闭</NButton></template>
        </WwModal>
      </NCard>

      <!-- 综合示例 -->
      <NCard title="综合功能弹窗">
        <NButton type="primary" @click="showComprehensive = true">打开综合功能弹窗</NButton>
        <WwModal
          v-model:show="showComprehensive"
          title="综合功能弹窗"
          :width="600"
          :height="400"
          :resizable="true"
          :maximizable="true"
          :minimizable="true"
          :draggable="true"
          :extra-buttons="extraButtons"
          padding="20px"
          title-font-size="18px"
        >
          <div>这个弹窗集成了所有功能：</div>
          <ul>
            <li>可缩放（拖动边框）</li>
            <li>可最大化/还原</li>
            <li>可最小化到右下角</li>
            <li>可拖拽移动</li>
            <li>自定义按钮（刷新、下载）</li>
            <li>自定义样式</li>
          </ul>
          <NDivider />
          <NInput v-model:value="form.name" placeholder="测试输入" />
          <template #footer>
            <NButton @click="showComprehensive = false">取消</NButton>
            <NButton type="primary" @click="showComprehensive = false">确定</NButton>
          </template>
        </WwModal>
      </NCard>
    </NSpace>
  </div>
</template>

<style scoped>
.modal-test-page {
  min-height: 100%;
}
</style>
