<script setup lang="ts">
import { computed, h, onMounted, ref } from 'vue'
import {
  NButton, NCard, NDataTable, NForm, NFormItem, NInput, NInputNumber, NModal, NPopconfirm,
  NRadio, NRadioGroup, NSelect, NSpace, NTag, NTree, useMessage,
  type DataTableColumns, type TreeOption
} from 'naive-ui'
import {
  assignRoleMenusApi, createRoleApi, deleteRoleApi, listRolesApi, menuTreeApi,
  updateRoleApi,
  type SysMenuNode, type SysRole
} from '@/api/system'
import { useAuth } from '@/composables/useAuth'

defineOptions({ name: 'RoleManage' })

const message = useMessage()
const auth = useAuth()

const roles = ref<SysRole[]>([])
const loading = ref(false)

const DATA_SCOPE_OPTIONS = [
  { label: '全部数据', value: 1 },
  { label: '本部门数据', value: 2 },
  { label: '本人数据', value: 3 }
]

const columns = computed<DataTableColumns<SysRole>>(() => [
  { title: '角色名称', key: 'roleName', width: 140 },
  { title: '角色标识', key: 'roleKey', width: 140 },
  { title: '排序', key: 'roleSort', width: 70 },
  {
    title: '数据范围', key: 'dataScope', width: 110,
    render: (row) =>
      DATA_SCOPE_OPTIONS.find((s) => s.value === row.dataScope)?.label || row.dataScope
  },
  {
    title: '状态', key: 'status', width: 80,
    render: (row) =>
      h(NTag, { size: 'small', type: row.status === 1 ? 'success' : 'error' },
        () => (row.status === 1 ? '正常' : '停用'))
  },
  { title: '备注', key: 'remark', render: (row) => row.remark || '-' },
  {
    title: '操作', key: 'actions', width: 230, fixed: 'right',
    render: (row) =>
      h(NSpace, { size: 4 }, () => [
        auth.hasPermission('system:role:edit')
          ? h(NButton, { size: 'small', quaternary: true, onClick: () => openEdit(row) }, () => '编辑')
          : null,
        auth.hasPermission('system:role:edit')
          ? h(NButton, { size: 'small', quaternary: true, onClick: () => openAssign(row) }, () => '菜单授权')
          : null,
        auth.hasPermission('system:role:remove')
          ? h(NPopconfirm, { onPositiveClick: () => handleDelete(row) }, {
              trigger: () => h(NButton, { size: 'small', quaternary: true, type: 'error' }, () => '删除'),
              default: () => `确认删除角色 ${row.roleName}？`
            })
          : null
      ])
  }
])

async function loadRoles() {
  loading.value = true
  try {
    roles.value = (await listRolesApi()).items
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
  roleName: '', roleKey: '', roleSort: 0, status: 1, dataScope: 1, remark: ''
})

function openCreate() {
  isEdit.value = false
  editingId.value = 0
  form.value = { roleName: '', roleKey: '', roleSort: 0, status: 1, dataScope: 1, remark: '' }
  modalVisible.value = true
}

function openEdit(row: SysRole) {
  isEdit.value = true
  editingId.value = row.roleId
  form.value = {
    roleName: row.roleName, roleKey: row.roleKey, roleSort: row.roleSort,
    status: row.status, dataScope: row.dataScope, remark: row.remark || ''
  }
  modalVisible.value = true
}

async function handleSubmit() {
  if (!form.value.roleName.trim()) {
    message.warning('请填写角色名称')
    return
  }
  try {
    if (isEdit.value) {
      await updateRoleApi(editingId.value, {
        roleName: form.value.roleName,
        roleSort: form.value.roleSort,
        status: form.value.status,
        dataScope: form.value.dataScope,
        remark: form.value.remark || null
      })
      message.success('角色已更新')
    } else {
      if (!form.value.roleKey.trim()) {
        message.warning('请填写角色标识')
        return
      }
      await createRoleApi(form.value)
      message.success('角色已创建')
    }
    modalVisible.value = false
    await loadRoles()
  } catch (e: any) {
    message.error(e.message)
  }
}

async function handleDelete(row: SysRole) {
  try {
    await deleteRoleApi(row.roleId)
    message.success('角色已删除')
    await loadRoles()
  } catch (e: any) {
    message.error(e.message)
  }
}

// ------------------------------------------------------------------
// 菜单授权
// ------------------------------------------------------------------

const assignVisible = ref(false)
const assignRoleId = ref(0)
const assignRoleName = ref('')
const menuTreeData = ref<TreeOption[]>([])
const checkedKeys = ref<number[]>([])

function toTree(nodes: SysMenuNode[]): TreeOption[] {
  return nodes.map((node) => ({
    key: node.menuId,
    label: node.perms ? `${node.menuName}（${node.perms}）` : node.menuName,
    children: node.children ? toTree(node.children) : undefined
  }))
}

async function openAssign(row: SysRole) {
  assignRoleId.value = row.roleId
  assignRoleName.value = row.roleName
  const tree = await menuTreeApi()
  menuTreeData.value = toTree(tree.items)
  checkedKeys.value = [...row.menuIds]
  assignVisible.value = true
}

async function handleAssignSubmit() {
  try {
    await assignRoleMenusApi(assignRoleId.value, checkedKeys.value)
    message.success('菜单权限已更新')
    assignVisible.value = false
    await loadRoles()
  } catch (e: any) {
    message.error(e.message)
  }
}

onMounted(loadRoles)
</script>

<template>
  <n-card :bordered="false">
    <n-space class="mb-3">
      <n-button v-if="auth.hasPermission('system:role:add')" type="primary" @click="openCreate">
        新增角色
      </n-button>
      <n-button @click="loadRoles">刷新</n-button>
    </n-space>

    <n-data-table
      :columns="columns"
      :data="roles"
      :loading="loading"
      :pagination="{ pageSize: 20 }"
      :scroll-x="900"
    />
  </n-card>

  <!-- 新增/编辑弹窗 -->
  <n-modal v-model:show="modalVisible" :title="isEdit ? '编辑角色' : '新增角色'" preset="card" style="width: 520px;">
    <n-form label-placement="left" label-width="80">
      <n-form-item label="角色名称">
        <n-input v-model:value="form.roleName" placeholder="显示名称" />
      </n-form-item>
      <n-form-item label="角色标识" v-if="!isEdit">
        <n-input v-model:value="form.roleKey" placeholder="如 viewer，创建后不可修改" />
      </n-form-item>
      <n-form-item label="角色标识" v-else>
        <n-input :value="form.roleKey" disabled />
      </n-form-item>
      <n-form-item label="显示排序">
        <n-input-number v-model:value="form.roleSort" :min="0" />
      </n-form-item>
      <n-form-item label="数据范围">
        <n-select v-model:value="form.dataScope" :options="DATA_SCOPE_OPTIONS" style="width: 200px;" />
      </n-form-item>
      <n-form-item label="状态">
        <n-radio-group v-model:value="form.status">
          <n-radio :value="1">正常</n-radio>
          <n-radio :value="0">停用</n-radio>
        </n-radio-group>
      </n-form-item>
      <n-form-item label="备注">
        <n-input v-model:value="form.remark" type="textarea" placeholder="可选" />
      </n-form-item>
    </n-form>
    <template #footer>
      <n-space justify="end">
        <n-button @click="modalVisible = false">取消</n-button>
        <n-button type="primary" @click="handleSubmit">确定</n-button>
      </n-space>
    </template>
  </n-modal>

  <!-- 菜单授权弹窗 -->
  <n-modal v-model:show="assignVisible" :title="`菜单授权 - ${assignRoleName}`" preset="card" style="width: 520px;">
    <n-tree
      block-line
      checkable
      cascade
      :data="menuTreeData"
      v-model:checked-keys="checkedKeys"
      :default-expand-all="false"
      :key-field="'key'"
      style="max-height: 480px; overflow: auto;"
    />
    <template #footer>
      <n-space justify="end">
        <n-button @click="assignVisible = false">取消</n-button>
        <n-button type="primary" @click="handleAssignSubmit">保存授权</n-button>
      </n-space>
    </template>
  </n-modal>
</template>
