<script setup lang="ts">
import {
  computed,
  h,
  onMounted,
  reactive,
  ref,
} from 'vue';

import { Plus, RotateCw } from '@vben/icons';

import {
  type DataTableColumns,
  type FormRules,
  NButton,
  NCard,
  NDataTable,
  NForm,
  NFormItem,
  NIcon,
  NInput,
  NPopconfirm,
  NSelect,
  NSpace,
  NSwitch,
  NTag,
} from 'naive-ui';

import { message } from '#/adapter/naive';
import {
  assignRolesApi,
  deleteUserApi,
  getDeptListApi,
  getRoleListApi,
  getUserPageApi,
  resetPasswordApi,
  saveUserApi,
  type SysDept,
  type SysRole,
  type SysUser,
} from '#/api/core/sys-user';
import { WwModal } from '#/components/WwModal';

defineOptions({ name: 'SystemUser' });

const loading = ref(false);
const users = ref<SysUser[]>([]);
const total = ref(0);
const page = ref(1);
const pageSize = ref(10);
const searchUsername = ref('');

const showFormModal = ref(false);
const showRoleModal = ref(false);
const showPwdModal = ref(false);
const isEdit = ref(false);

const formData = reactive<{
  deptId: null | number;
  id: null | number;
  nickname: string;
  plainPassword: string;
  status: number;
  username: string;
}>({
  id: null,
  username: '',
  nickname: '',
  plainPassword: '',
  deptId: null,
  status: 1,
});

const roleForm = reactive<{
  roleIds: number[];
  userId: null | number;
}>({
  userId: null,
  roleIds: [],
});

const pwdForm = reactive<{
  password: string;
  userId: null | number;
}>({
  userId: null,
  password: '',
});

const deptList = ref<SysDept[]>([]);
const roleList = ref<SysRole[]>([]);

const formRef = ref();
const pwdFormRef = ref();

const formRules: FormRules = {
  username: [
    { required: true, message: '请输入用户名', trigger: 'blur' },
    { min: 4, max: 20, message: '用户名长度为4-20位', trigger: 'blur' },
    {
      pattern: /^[0-9a-zA-Z_\u4e00-\u9fa5]+$/,
      message: '用户名只能包含字母、数字、下划线和中文',
      trigger: 'blur',
    },
  ],
  nickname: [
    { required: true, message: '请输入昵称', trigger: 'blur' },
    { max: 30, message: '昵称最长30位', trigger: 'blur' },
  ],
  plainPassword: [
    {
      validator: (_rule: any, value: string) => {
        if (!isEdit.value && (!value || value.length === 0)) {
          return new Error('请输入密码');
        }
        if (value && value.length > 0 && value.length < 6) {
          return new Error('密码长度不能少于6位');
        }
        return true;
      },
      trigger: 'blur',
    },
  ],
};

const pwdRules: FormRules = {
  password: [
    { required: true, message: '请输入新密码', trigger: 'blur' },
    { min: 6, message: '密码长度不能少于6位', trigger: 'blur' },
  ],
};

const deptOptions = computed(() => {
  return deptList.value
    .filter((d) => d.status === 1)
    .map((d) => ({
      label: d.deptName,
      value: d.id,
    }));
});

const roleOptions = computed(() => {
  return roleList.value
    .filter((r) => r.status === 1)
    .map((r) => ({
      label: `${r.roleName} (${r.roleKey})`,
      value: r.id,
    }));
});

const columns: DataTableColumns<SysUser> = [
  { type: 'selection', width: 50 },
  { title: 'ID', key: 'id', width: 120 },
  { title: '用户名', key: 'username', width: 140 },
  { title: '昵称', key: 'nickname', width: 140 },
  {
    title: '部门',
    key: 'dept',
    width: 140,
    render(row) {
      return row.dept?.deptName || '-';
    },
  },
  {
    title: '角色',
    key: 'roles',
    render(row) {
      if (!row.roles || row.roles.length === 0) return '-';
      return h(
        NSpace,
        { size: 'small', wrap: true },
        () =>
          row.roles!.map((r) =>
            h(
              NTag,
              { size: 'small', type: 'info' },
              { default: () => r.roleName },
            ),
          ),
      );
    },
  },
  {
    title: '状态',
    key: 'status',
    width: 100,
    render(row) {
      return h(NTag, {
        type: row.status === 1 ? 'success' : 'error',
        size: 'small',
      }, {
        default: () => (row.status === 1 ? '正常' : '禁用'),
      });
    },
  },
  { title: '创建时间', key: 'createdDate', width: 180 },
  {
    title: '操作',
    key: 'actions',
    width: 320,
    fixed: 'right',
    render(row) {
      return h(NSpace, { size: 'small' }, () => [
        h(
          NButton,
          {
            size: 'small',
            type: 'primary',
            quaternary: true,
            onClick: () => handleEdit(row),
          },
          { default: () => '编辑' },
        ),
        h(
          NButton,
          {
            size: 'small',
            type: 'info',
            quaternary: true,
            onClick: () => handleAssignRoles(row),
          },
          { default: () => '分配角色' },
        ),
        h(
          NButton,
          {
            size: 'small',
            type: 'warning',
            quaternary: true,
            onClick: () => handleResetPwd(row),
          },
          { default: () => '重置密码' },
        ),
        h(
          NPopconfirm,
          {
            onPositiveClick: () => handleDelete(row.id!),
          },
          {
            trigger: () =>
              h(
                NButton,
                { size: 'small', type: 'error', quaternary: true },
                { default: () => '删除' },
              ),
            default: () => `确定删除用户 ${row.username} 吗？`,
          },
        ),
      ]);
    },
  },
];

async function fetchData() {
  loading.value = true;
  try {
    const result = await getUserPageApi({
      pageNumber: page.value,
      pageSize: pageSize.value,
      username: searchUsername.value || undefined,
    });
    users.value = result.items;
    total.value = result.total;
  } finally {
    loading.value = false;
  }
}

async function fetchDepts() {
  try {
    deptList.value = await getDeptListApi();
  } catch {
    // ignore - dept list optional
  }
}

async function fetchRoles() {
  try {
    roleList.value = await getRoleListApi();
  } catch {
    // ignore - role list optional
  }
}

function handleAdd() {
  isEdit.value = false;
  Object.assign(formData, {
    id: null,
    username: '',
    nickname: '',
    plainPassword: '',
    deptId: null,
    status: 1,
  });
  showFormModal.value = true;
}

function handleEdit(row: SysUser) {
  isEdit.value = true;
  Object.assign(formData, {
    id: row.id,
    username: row.username,
    nickname: row.nickname,
    plainPassword: '',
    deptId: row.dept?.id ?? null,
    status: row.status ?? 1,
  });
  showFormModal.value = true;
}

async function handleSubmitForm() {
  // Explicit guard: block submit when form is not ready
  const form = formRef.value;
  if (!form) return;
  try {
    await form.validate();
  } catch {
    return;
  }
  const data: Record<string, any> = {
    id: formData.id,
    username: formData.username,
    nickname: formData.nickname,
    status: formData.status,
    plainPassword: formData.plainPassword || undefined,
    dept: formData.deptId ? { id: formData.deptId } : null,
  };
  await saveUserApi(data);
  message.success(isEdit.value ? '更新成功' : '创建成功');
  showFormModal.value = false;
  await fetchData();
}

async function handleDelete(id: number) {
  await deleteUserApi(id);
  message.success('删除成功');
  await fetchData();
}

function handleResetPwd(row: SysUser) {
  Object.assign(pwdForm, {
    userId: row.id,
    password: '',
  });
  showPwdModal.value = true;
}

async function handleSubmitPwd() {
  // Explicit guard: block submit when form is not ready
  const form = pwdFormRef.value;
  if (!form) return;
  try {
    await form.validate();
  } catch {
    return;
  }
  await resetPasswordApi(pwdForm.userId!, pwdForm.password);
  message.success('密码重置成功');
  showPwdModal.value = false;
}

function handleAssignRoles(row: SysUser) {
  Object.assign(roleForm, {
    userId: row.id,
    roleIds: (row.roles || []).map((r) => r.id),
  });
  showRoleModal.value = true;
}

async function handleSubmitRoles() {
  await assignRolesApi(roleForm.userId!, roleForm.roleIds);
  message.success('角色分配成功');
  showRoleModal.value = false;
  await fetchData();
}

function handleSearch() {
  page.value = 1;
  fetchData();
}

function handleReset() {
  searchUsername.value = '';
  page.value = 1;
  fetchData();
}

function handlePageChange(p: number) {
  page.value = p;
  fetchData();
}

function handlePageSizeChange(ps: number) {
  pageSize.value = ps;
  page.value = 1;
  fetchData();
}

onMounted(() => {
  fetchData();
  fetchDepts();
  fetchRoles();
});
</script>

<template>
  <div class="p-2 flex h-full flex-col">
    <NCard title="" :bordered="false" class="flex-1 rounded-lg">
      <!-- 搜索栏 -->
      <div class="mb-3 flex items-center gap-2">
        <n-input-group>
          <NInput :style="{ width: '220px' }" v-model:value="searchUsername" placeholder="输入用户名搜索" clearable @keyup.enter="handleSearch" />
          <NButton @click="handleSearch">
            搜索
          </NButton>
        </n-input-group>
        <NButton @click="handleReset">
          <template #icon>
            <NIcon><RotateCw /></NIcon>
          </template>
        </NButton>
        <div class="flex-1"></div>
        <NButton type="primary" @click="handleAdd">
          <template #icon>
            <NIcon><Plus /></NIcon>
          </template>
          新增
        </NButton>
      </div>

      <!-- 数据表格 -->
      <NDataTable
        :columns="columns"
        :data="users"
        :loading="loading"
        bordered
        :pagination="{
          page,
          pageSize,
          showSizePicker: true,
          pageSizes: [10, 20, 50, 100],
          itemCount: total,
          onChange: (p: number) => handlePageChange(p),
          onUpdatePageSize: (ps: number) => handlePageSizeChange(ps),
        }"
        :scroll-x="1200"
        :single-line="false"
      />
    </NCard>

    <!-- 新增/编辑用户弹窗 -->
    <WwModal
      v-model:show="showFormModal"
      :title="isEdit ? '编辑用户' : '新增用户'"
      :width="480"
      :mask-closable="false"
      confirm-text="确定"
      cancel-text="取消"
      :maximizable="false"
      :resizable="false"
      @confirm="handleSubmitForm"
    >
      <NForm
        ref="formRef"
        :model="formData"
        :rules="formRules"
        label-placement="left"
        label-width="80px"
      >
        <NFormItem label="用户名" path="username">
          <NInput
            v-model:value="formData.username"
            placeholder="请输入用户名"
            :disabled="isEdit"
          />
        </NFormItem>
        <NFormItem label="昵称" path="nickname">
          <NInput v-model:value="formData.nickname" placeholder="请输入昵称" />
        </NFormItem>
        <NFormItem label="密码" path="plainPassword">
          <NInput
            v-model:value="formData.plainPassword"
            type="password"
            show-password-on="click"
            :placeholder="isEdit ? '不修改请留空' : '请输入密码'"
          />
        </NFormItem>
        <NFormItem label="部门">
          <NSelect
            v-model:value="formData.deptId"
            :options="deptOptions"
            placeholder="请选择部门"
            clearable
            filterable
          />
        </NFormItem>
        <NFormItem label="状态">
          <NSwitch
            :value="formData.status === 1"
            @update:value="(v: boolean) => (formData.status = v ? 1 : 0)"
          >
            <template #checked>正常</template>
            <template #unchecked>禁用</template>
          </NSwitch>
        </NFormItem>
      </NForm>
    </WwModal>

    <!-- 分配角色弹窗 -->
    <WwModal
      v-model:show="showRoleModal"
      title="分配角色"
      :width="480"
      :mask-closable="false"
      confirm-text="确定"
      cancel-text="取消"
      :maximizable="false"
      :resizable="false"
      @confirm="handleSubmitRoles"
    >
      <NForm
        :model="roleForm"
        label-placement="left"
        label-width="80px"
      >
        <NFormItem label="角色">
          <NSelect
            v-model:value="roleForm.roleIds"
            :options="roleOptions"
            multiple
            placeholder="请选择角色"
            filterable
          />
        </NFormItem>
      </NForm>
    </WwModal>

    <!-- 重置密码弹窗 -->
    <WwModal
      v-model:show="showPwdModal"
      title="重置密码"
      :width="420"
      :mask-closable="false"
      confirm-text="确定"
      cancel-text="取消"
      :maximizable="false"
      :resizable="false"
      @confirm="handleSubmitPwd"
    >
      <NForm
        ref="pwdFormRef"
        :model="pwdForm"
        :rules="pwdRules"
        label-placement="left"
        label-width="80px"
      >
        <NFormItem label="新密码" path="password">
          <NInput
            v-model:value="pwdForm.password"
            type="password"
            show-password-on="click"
            placeholder="请输入新密码（至少6位）"
          />
        </NFormItem>
      </NForm>
    </WwModal>
  </div>
</template>
