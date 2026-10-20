<script setup lang="ts">
import { computed, h, onMounted, ref } from 'vue'
import {
  NButton, NCard, NDataTable, NForm, NFormItem, NInput, NInputNumber, NModal, NPopconfirm,
  NRadio, NRadioGroup, NSelect, NSpace, NTag, useMessage, type DataTableColumns
} from 'naive-ui'
import {
  createUserApi, deleteUserApi, getUserPageApi, listDeptsApi, listRolesApi,
  resetPasswordApi, updateUserApi,
  type SysDept, type SysRole, type SysUser, type SysUserPayload
} from '@/api/system'
import { useAuth } from '@/composables/useAuth'

defineOptions({ name: 'UserManage' })

const message = useMessage()
const auth = useAuth()

const users = ref<SysUser[]>([])
const total = ref(0)
const page = ref(1)
const size = ref(10)
const keyword = ref('')
const loading = ref(false)

const depts = ref<SysDept[]>([])
const roles = ref<SysRole[]>([])

const deptOptions = computed(() =>
  depts.value.map((d) => ({ label: d.deptName, value: d.deptId }))
)
const roleOptions = computed(() =>
  roles.value.map((r) => ({ label: r.roleName, value: r.roleId }))
)

const columns = computed<DataTableColumns<SysUser>>(() => [
  { title: '用户名', key: 'username', width: 120 },
  { title: '昵称', key: 'nickname', width: 120 },
  {
    title: '部门', key: 'deptName', width: 120,
    render: (row) => row.deptName || '-'
  },
  {
    title: '角色', key: 'roleNames',
    render: (row) =>
      row.roleNames.length
        ? h(NSpace, { size: 4 }, () =>
            row.roleNames.map((name) => h(NTag, { size: 'small', type: 'info' }, () => name)))
        : '-'
  },
  {
    title: '状态', key: 'status', width: 80,
    render: (row) =>
      h(NTag, { size: 'small', type: row.status === 1 ? 'success' : 'error' },
        () => (row.status === 1 ? '正常' : '停用'))
  },
  {
    title: '最近登录', key: 'loginAt', width: 180,
    render: (row) => (row.loginAt ? new Date(row.loginAt).toLocaleString() : '-')
  },
  {
    title: '操作', key: 'actions', width: 200, fixed: 'right',
    render: (row) =>
      h(NSpace, { size: 4 }, () => [
        auth.hasPermission('system:user:edit')
          ? h(NButton, { size: 'small', quaternary: true, onClick: () => openEdit(row) }, () => '编辑')
          : null,
        auth.hasPermission('system:user:reset')
          ? h(NButton, { size: 'small', quaternary: true, onClick: () => openReset(row) }, () => '重置密码')
          : null,
        auth.hasPermission('system:user:remove')
          ? h(NPopconfirm, { onPositiveClick: () => handleDelete(row) }, {
              trigger: () => h(NButton, { size: 'small', quaternary: true, type: 'error' }, () => '删除'),
              default: () => `确认删除用户 ${row.username}？`
            })
          : null
      ])
  }
])

async function loadUsers() {
  loading.value = true
  try {
    const result = await getUserPageApi({
      keyword: keyword.value || undefined,
      page: page.value,
      size: size.value
    })
    users.value = result.items
    total.value = result.total
  } finally {
    loading.value = false
  }
}

async function loadOptions() {
  const [deptResult, roleResult] = await Promise.all([listDeptsApi(), listRolesApi()])
  depts.value = deptResult.items
  roles.value = roleResult.items
}

// ------------------------------------------------------------------
// 新增 / 编辑
// ------------------------------------------------------------------

const modalVisible = ref(false)
const isEdit = ref(false)
const form = ref<SysUserPayload>(emptyForm())

function emptyForm(): SysUserPayload {
  return {
    nickname: '', deptId: 0, email: '', phone: '', gender: 0,
    status: 1, remark: '', roleIds: []
  }
}

function openCreate() {
  isEdit.value = false
  form.value = emptyForm()
  modalVisible.value = true
}

function openEdit(row: SysUser) {
  isEdit.value = true
  form.value = {
    nickname: row.nickname,
    deptId: row.deptId,
    email: row.email || '',
    phone: row.phone || '',
    gender: row.gender,
    status: row.status,
    remark: row.remark || '',
    roleIds: [...row.roleIds]
  }
  editingId.value = row.userId
  editingUsername.value = row.username
  modalVisible.value = true
}

const editingId = ref(0)
const editingUsername = ref('')

async function handleSubmit() {
  if (!form.value.nickname.trim()) {
    message.warning('请填写昵称')
    return
  }
  try {
    if (isEdit.value) {
      await updateUserApi(editingId.value, form.value)
      message.success('用户已更新')
    } else {
      await createUserApi({ ...form.value, username: formUsername.value })
      message.success('用户已创建，默认密码 123456')
    }
    modalVisible.value = false
    await loadUsers()
  } catch (e: any) {
    message.error(e.message)
  }
}

const formUsername = ref('')

// ------------------------------------------------------------------
// 重置密码
// ------------------------------------------------------------------

const resetVisible = ref(false)
const resetForm = ref({ userId: 0, username: '', password: '' })

function openReset(row: SysUser) {
  resetForm.value = { userId: row.userId, username: row.username, password: '' }
  resetVisible.value = true
}

async function handleResetSubmit() {
  if (!resetForm.value.password) {
    message.warning('请输入新密码')
    return
  }
  try {
    await resetPasswordApi(resetForm.value.userId, resetForm.value.password)
    message.success('密码已重置')
    resetVisible.value = false
  } catch (e: any) {
    message.error(e.message)
  }
}

// ------------------------------------------------------------------
// 删除
// ------------------------------------------------------------------

async function handleDelete(row: SysUser) {
  try {
    await deleteUserApi(row.userId)
    message.success('用户已删除')
    await loadUsers()
  } catch (e: any) {
    message.error(e.message)
  }
}

onMounted(async () => {
  await loadOptions()
  await loadUsers()
})
</script>

<template>
  <n-card :bordered="false">
    <n-space class="mb-3" justify="space-between">
      <n-space>
        <n-input v-model:value="keyword" placeholder="用户名/昵称" clearable style="width: 200px;" @keyup.enter="loadUsers" />
        <n-button @click="loadUsers">查询</n-button>
      </n-space>
      <n-button v-if="auth.hasPermission('system:user:add')" type="primary" @click="openCreate">
        新增用户
      </n-button>
    </n-space>

    <n-data-table
      :columns="columns"
      :data="users"
      :loading="loading"
      :pagination="{
        page, pageSize: size, itemCount: total,
        showSizePicker: true, pageSizes: [10, 20, 50],
        onChange: (p: number) => { page = p; loadUsers() },
        onUpdatePageSize: (s: number) => { size = s; page = 1; loadUsers() }
      }"
      remote
      :scroll-x="1000"
    />
  </n-card>

  <!-- 新增/编辑弹窗 -->
  <n-modal v-model:show="modalVisible" :title="isEdit ? '编辑用户' : '新增用户'" preset="card" style="width: 520px;">
    <n-form label-placement="left" label-width="80">
      <n-form-item label="用户名" v-if="!isEdit">
        <n-input v-model:value="formUsername" placeholder="登录用户名" />
      </n-form-item>
      <n-form-item label="用户名" v-else>
        <n-input :value="editingUsername" disabled />
      </n-form-item>
      <n-form-item label="昵称">
        <n-input v-model:value="form.nickname" placeholder="显示昵称" />
      </n-form-item>
      <n-form-item label="部门">
        <n-select v-model:value="form.deptId" :options="deptOptions" />
      </n-form-item>
      <n-form-item label="角色">
        <n-select v-model:value="form.roleIds" multiple :options="roleOptions" />
      </n-form-item>
      <n-form-item label="邮箱">
        <n-input v-model:value="form.email" placeholder="可选" />
      </n-form-item>
      <n-form-item label="手机号">
        <n-input v-model:value="form.phone" placeholder="可选" />
      </n-form-item>
      <n-form-item label="性别">
        <n-radio-group v-model:value="form.gender">
          <n-radio :value="0">未知</n-radio>
          <n-radio :value="1">男</n-radio>
          <n-radio :value="2">女</n-radio>
        </n-radio-group>
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

  <!-- 重置密码弹窗 -->
  <n-modal v-model:show="resetVisible" title="重置密码" preset="card" style="width: 420px;">
    <n-form label-placement="left" label-width="80">
      <n-form-item label="用户名">
        <n-input :value="resetForm.username" disabled />
      </n-form-item>
      <n-form-item label="新密码">
        <n-input v-model:value="resetForm.password" type="password" show-password-on="click" placeholder="请输入新密码" />
      </n-form-item>
    </n-form>
    <template #footer>
      <n-space justify="end">
        <n-button @click="resetVisible = false">取消</n-button>
        <n-button type="primary" @click="handleResetSubmit">确定</n-button>
      </n-space>
    </template>
  </n-modal>
</template>
