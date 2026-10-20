<script setup lang="ts">
import { computed, h, onMounted, ref } from 'vue'
import {
  NButton, NCard, NDataTable, NForm, NFormItem, NInput, NInputNumber, NModal, NPopconfirm,
  NRadio, NRadioGroup, NSelect, NSpace, NTag, useMessage, type DataTableColumns
} from 'naive-ui'
import {
  createDeptApi, deleteDeptApi, listDeptsApi, updateDeptApi, type SysDept
} from '@/api/system'
import { useAuth } from '@/composables/useAuth'

defineOptions({ name: 'DeptManage' })

const message = useMessage()
const auth = useAuth()

interface DeptRow extends SysDept {
  children?: DeptRow[]
}

const depts = ref<SysDept[]>([])
const loading = ref(false)

const columns = computed<DataTableColumns<DeptRow>>(() => [
  { title: '部门名称', key: 'deptName' },
  { title: '排序', key: 'orderNum', width: 80 },
  { title: '负责人', key: 'leader', width: 100, render: (row) => row.leader || '-' },
  { title: '联系电话', key: 'phone', width: 130, render: (row) => row.phone || '-' },
  {
    title: '状态', key: 'status', width: 80,
    render: (row) =>
      h(NTag, { size: 'small', type: row.status === 1 ? 'success' : 'error' },
        () => (row.status === 1 ? '正常' : '停用'))
  },
  {
    title: '操作', key: 'actions', width: 160, fixed: 'right',
    render: (row) =>
      h(NSpace, { size: 4 }, () => [
        auth.hasPermission('system:dept:edit')
          ? h(NButton, { size: 'small', quaternary: true, onClick: () => openEdit(row) }, () => '编辑')
          : null,
        auth.hasPermission('system:dept:remove')
          ? h(NPopconfirm, { onPositiveClick: () => handleDelete(row) }, {
              trigger: () => h(NButton, { size: 'small', quaternary: true, type: 'error' }, () => '删除'),
              default: () => `确认删除部门 ${row.deptName}？`
            })
          : null
      ])
  }
])

/** 平铺部门 → 树（NDataTable 按 children 渲染树表）。 */
const treeData = computed<DeptRow[]>(() => {
  const byId = new Map<number, DeptRow>()
  for (const dept of depts.value) {
    byId.set(dept.deptId, { ...dept })
  }
  const roots: DeptRow[] = []
  for (const row of byId.values()) {
    if (row.parentId && byId.has(row.parentId)) {
      const parent = byId.get(row.parentId)!
      parent.children = parent.children || []
      parent.children.push(row)
    } else {
      roots.push(row)
    }
  }
  const order = (nodes: DeptRow[]) => {
    nodes.sort((a, b) => a.orderNum - b.orderNum)
    nodes.forEach((n) => n.children && order(n.children))
  }
  order(roots)
  return roots
})

const parentOptions = computed(() => [
  { label: '顶级部门', value: 0 },
  ...depts.value
    .filter((d) => d.deptId !== editingId.value)
    .map((d) => ({ label: d.deptName, value: d.deptId }))
])

async function loadDepts() {
  loading.value = true
  try {
    depts.value = (await listDeptsApi()).items
  } finally {
    loading.value = false
  }
}

// ------------------------------------------------------------------
// 新增 / 编辑
// ------------------------------------------------------------------

const modalVisible = ref(false)
const isEdit = ref(false)
const editingId = ref(0)
const form = ref({
  parentId: 0, deptName: '', orderNum: 0, leader: '', phone: '', status: 1
})

function openCreate() {
  isEdit.value = false
  editingId.value = 0
  form.value = { parentId: 0, deptName: '', orderNum: 0, leader: '', phone: '', status: 1 }
  modalVisible.value = true
}

function openEdit(row: SysDept) {
  isEdit.value = true
  editingId.value = row.deptId
  form.value = {
    parentId: row.parentId, deptName: row.deptName, orderNum: row.orderNum,
    leader: row.leader || '', phone: row.phone || '', status: row.status
  }
  modalVisible.value = true
}

async function handleSubmit() {
  if (!form.value.deptName.trim()) {
    message.warning('请填写部门名称')
    return
  }
  const payload = {
    parentId: form.value.parentId,
    deptName: form.value.deptName.trim(),
    orderNum: form.value.orderNum,
    leader: form.value.leader || null,
    phone: form.value.phone || null,
    status: form.value.status
  }
  try {
    if (isEdit.value) {
      await updateDeptApi(editingId.value, payload)
      message.success('部门已更新')
    } else {
      await createDeptApi(payload)
      message.success('部门已创建')
    }
    modalVisible.value = false
    await loadDepts()
  } catch (e: any) {
    message.error(e.message)
  }
}

async function handleDelete(row: SysDept) {
  try {
    await deleteDeptApi(row.deptId)
    message.success('部门已删除')
    await loadDepts()
  } catch (e: any) {
    message.error(e.message)
  }
}

onMounted(loadDepts)
</script>

<template>
  <n-card :bordered="false">
    <n-space class="mb-3">
      <n-button v-if="auth.hasPermission('system:dept:add')" type="primary" @click="openCreate">
        新增部门
      </n-button>
      <n-button @click="loadDepts">刷新</n-button>
    </n-space>

    <n-data-table
      :columns="columns"
      :data="treeData"
      :loading="loading"
      :pagination="false"
      :default-expand-all="true"
      :scroll-x="800"
    />
  </n-card>

  <n-modal v-model:show="modalVisible" :title="isEdit ? '编辑部门' : '新增部门'" preset="card" style="width: 500px;">
    <n-form label-placement="left" label-width="80">
      <n-form-item label="上级部门">
        <n-select v-model:value="form.parentId" :options="parentOptions" />
      </n-form-item>
      <n-form-item label="部门名称">
        <n-input v-model:value="form.deptName" placeholder="请输入部门名称" />
      </n-form-item>
      <n-form-item label="显示排序">
        <n-input-number v-model:value="form.orderNum" :min="0" />
      </n-form-item>
      <n-form-item label="负责人">
        <n-input v-model:value="form.leader" placeholder="可选" />
      </n-form-item>
      <n-form-item label="联系电话">
        <n-input v-model:value="form.phone" placeholder="可选" />
      </n-form-item>
      <n-form-item label="状态">
        <n-radio-group v-model:value="form.status">
          <n-radio :value="1">正常</n-radio>
          <n-radio :value="0">停用</n-radio>
        </n-radio-group>
      </n-form-item>
    </n-form>
    <template #footer>
      <n-space justify="end">
        <n-button @click="modalVisible = false">取消</n-button>
        <n-button type="primary" @click="handleSubmit">确定</n-button>
      </n-space>
    </template>
  </n-modal>
</template>
