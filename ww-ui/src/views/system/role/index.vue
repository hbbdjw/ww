<script setup lang="ts">
import { computed, h, onMounted, reactive, ref } from 'vue';

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
  NTree,
  type TreeOption,
} from 'naive-ui';

import { message } from '#/adapter/naive';
import { getDeptListApi, type SysDept } from '#/api/core/sys-dept';
import { getMenuListApi, type SysMenu } from '#/api/core/sys-menu';
import {
  assignDeptsApi,
  assignMenusApi,
  deleteRoleApi,
  getRoleDeptIdsApi,
  getRoleListApi,
  getRoleMenuIdsApi,
  saveRoleApi,
  type SysRole,
} from '#/api/core/sys-role';
import { WwModal } from '#/components/WwModal';

defineOptions({ name: 'SystemRole' });

const loading = ref(false);
const roles = ref<SysRole[]>([]);
const menus = ref<SysMenu[]>([]);
const depts = ref<SysDept[]>([]);

const showFormModal = ref(false);
const showMenuModal = ref(false);
const showDeptModal = ref(false);
const isEdit = ref(false);

const dataScopeOptions = [
  { label: '全部数据', value: '1' },
  { label: '自定义数据', value: '2' },
  { label: '本部门数据', value: '3' },
  { label: '本部门及以下', value: '4' },
  { label: '仅本人数据', value: '5' },
];

const formData = reactive<{
  dataScope: string;
  id: null | number;
  orderNum: number;
  roleKey: string;
  roleName: string;
  status: number;
}>({
  id: null,
  roleName: '',
  roleKey: '',
  dataScope: '1',
  orderNum: 0,
  status: 1,
});

const menuForm = reactive<{
  checkedKeys: number[];
  roleId: null | number;
}>({
  roleId: null,
  checkedKeys: [],
});

const deptForm = reactive<{
  checkedKeys: number[];
  roleId: null | number;
}>({
  roleId: null,
  checkedKeys: [],
});

const formRef = ref();

const formRules: FormRules = {
  roleName: [
    { required: true, message: '请输入角色名称', trigger: 'blur' },
    { max: 30, message: '角色名称最长30位', trigger: 'blur' },
  ],
  roleKey: [
    { required: true, message: '请输入权限字符', trigger: 'blur' },
    { max: 100, message: '权限字符最长100位', trigger: 'blur' },
  ],
};

function buildMenuTree(menuList: SysMenu[]): TreeOption[] {
  const map = new Map<number, TreeOption>();
  for (const m of menuList) {
    const typeLabel = m.menuType === 'M' ? '目录' : m.menuType === 'C' ? '菜单' : '按钮';
    map.set(m.id, {
      key: m.id,
      label: `${m.menuName} (${typeLabel})`,
      children: [],
    });
  }
  const roots: TreeOption[] = [];
  for (const [id, node] of map) {
    const menu = menuList.find((m) => m.id === id);
    if (!menu) continue;
    if (!menu.parentId || menu.parentId === 0) {
      roots.push(node);
    } else {
      const parent = map.get(menu.parentId);
      if (parent) {
        (parent.children as TreeOption[]).push(node);
      } else {
        roots.push(node);
      }
    }
  }
  return roots;
}

const menuTree = computed(() => buildMenuTree(menus.value));

function buildDeptTree(deptList: SysDept[]): TreeOption[] {
  const map = new Map<number, TreeOption>();
  for (const d of deptList) {
    map.set(d.id, {
      key: d.id,
      label: d.deptName,
      children: [],
    });
  }
  const roots: TreeOption[] = [];
  for (const [id, node] of map) {
    const dept = deptList.find((d) => d.id === id);
    if (!dept) continue;
    if (!dept.parentId || dept.parentId === 0) {
      roots.push(node);
    } else {
      const parent = map.get(dept.parentId);
      if (parent) {
        (parent.children as TreeOption[]).push(node);
      } else {
        roots.push(node);
      }
    }
  }
  return roots;
}

const deptTree = computed(() => buildDeptTree(depts.value));

const columns: DataTableColumns<SysRole> = [
  { title: 'ID', key: 'id', width: 80 },
  { title: '角色名称', key: 'roleName', width: 140 },
  { title: '权限字符', key: 'roleKey', width: 160 },
  {
    title: '数据范围',
    key: 'dataScope',
    width: 120,
    render(row) {
      const scopeMap: Record<string, { label: string; type: 'default' | 'error' | 'info' | 'success' | 'warning' }> = {
        '1': { label: '全部数据', type: 'success' },
        '2': { label: '自定义', type: 'warning' },
        '3': { label: '本部门', type: 'info' },
        '4': { label: '本部门及以下', type: 'info' },
        '5': { label: '仅本人', type: 'default' },
      };
      const s = scopeMap[row.dataScope] || { label: '未知', type: 'default' };
      return h(NTag, { size: 'small', type: s.type }, { default: () => s.label });
    },
  },
  {
    title: '状态',
    key: 'status',
    width: 80,
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
    width: 280,
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
            onClick: () => handleAssignMenus(row),
          },
          { default: () => '分配权限' },
        ),
        h(
          NButton,
          {
            size: 'small',
            type: 'warning',
            quaternary: true,
            onClick: () => handleAssignDepts(row),
          },
          { default: () => '数据范围' },
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
            default: () => `确定删除角色 ${row.roleName} 吗？`,
          },
        ),
      ]);
    },
  },
];

async function fetchData() {
  loading.value = true;
  try {
    roles.value = await getRoleListApi();
  } finally {
    loading.value = false;
  }
}

async function fetchMenus() {
  try {
    menus.value = await getMenuListApi();
  } catch {
    // ignore
  }
}

async function fetchDepts() {
  try {
    depts.value = await getDeptListApi();
  } catch {
    // ignore
  }
}

function handleAdd() {
  isEdit.value = false;
  Object.assign(formData, {
    id: null,
    roleName: '',
    roleKey: '',
    dataScope: '1',
    orderNum: 0,
    status: 1,
  });
  showFormModal.value = true;
}

function handleEdit(row: SysRole) {
  isEdit.value = true;
  Object.assign(formData, {
    id: row.id,
    roleName: row.roleName,
    roleKey: row.roleKey,
    dataScope: row.dataScope || '1',
    orderNum: 0,
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
  await saveRoleApi({
    id: formData.id ?? undefined,
    roleName: formData.roleName,
    roleKey: formData.roleKey,
    dataScope: formData.dataScope,
    status: formData.status,
  });
  message.success(isEdit.value ? '更新成功' : '创建成功');
  showFormModal.value = false;
  await fetchData();
}

async function handleDelete(id: number) {
  await deleteRoleApi(id);
  message.success('删除成功');
  await fetchData();
}

async function handleAssignMenus(row: SysRole) {
  Object.assign(menuForm, {
    roleId: row.id,
    checkedKeys: [],
  });
  showMenuModal.value = true;
  try {
    menuForm.checkedKeys = await getRoleMenuIdsApi(row.id!);
  } catch {
    // ignore
  }
}

async function handleSubmitMenus() {
  await assignMenusApi(menuForm.roleId!, menuForm.checkedKeys);
  message.success('权限分配成功');
  showMenuModal.value = false;
  await fetchData();
}

async function handleAssignDepts(row: SysRole) {
  Object.assign(deptForm, {
    roleId: row.id,
    checkedKeys: [],
  });
  showDeptModal.value = true;
  try {
    deptForm.checkedKeys = await getRoleDeptIdsApi(row.id!);
  } catch {
    // ignore
  }
}

async function handleSubmitDepts() {
  await assignDeptsApi(deptForm.roleId!, deptForm.checkedKeys);
  message.success('数据范围设置成功');
  showDeptModal.value = false;
  await fetchData();
}

function handleMenuCheck(keys: Array<number | string>) {
  menuForm.checkedKeys = keys as number[];
}

function handleDeptCheck(keys: Array<number | string>) {
  deptForm.checkedKeys = keys as number[];
}

onMounted(() => {
  fetchData();
  fetchMenus();
  fetchDepts();
});
</script>

<template>
  <div class="p-2 flex h-full flex-col">
    <NCard title="" :bordered="false" class="flex-1 rounded-lg">
      <!-- 工具栏 -->
      <div class="mb-3 flex items-center gap-2">
        <div class="flex-1"></div>
        <NButton @click="fetchData">
          <template #icon>
            <NIcon><RotateCw /></NIcon>
          </template>
          刷新
        </NButton>
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
        :data="roles"
        :loading="loading"
        bordered
        :scroll-x="900"
        :single-line="false"
        :row-key="(row: SysRole) => row.id"
      />
    </NCard>

    <!-- 新增/编辑角色弹窗 -->
    <WwModal
      v-model:show="showFormModal"
      :title="isEdit ? '编辑角色' : '新增角色'"
      :width="500"
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
        label-width="100px"
      >
        <NFormItem label="角色名称" path="roleName">
          <NInput v-model:value="formData.roleName" placeholder="请输入角色名称" />
        </NFormItem>
        <NFormItem label="权限字符" path="roleKey">
          <NInput v-model:value="formData.roleKey" placeholder="如 admin, manager" />
        </NFormItem>
        <NFormItem label="数据范围">
          <NSelect
            v-model:value="formData.dataScope"
            :options="dataScopeOptions"
            placeholder="请选择数据范围"
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

    <!-- 分配菜单权限弹窗 -->
    <WwModal
      v-model:show="showMenuModal"
      title="分配菜单权限"
      :width="500"
      :mask-closable="false"
      confirm-text="确定"
      cancel-text="取消"
      :maximizable="false"
      :resizable="false"
      @confirm="handleSubmitMenus"
    >
      <div class="max-h-96 overflow-auto">
        <NTree
          :data="menuTree"
          checkable
          :checked-keys="menuForm.checkedKeys"
          @update:checked-keys="handleMenuCheck"
          default-expand-all
          :expand-on-click="false"
        />
      </div>
    </WwModal>

    <!-- 分配数据范围部门弹窗 -->
    <WwModal
      v-model:show="showDeptModal"
      title="分配数据范围（自定义）"
      :width="500"
      :mask-closable="false"
      confirm-text="确定"
      cancel-text="取消"
      :maximizable="false"
      :resizable="false"
      @confirm="handleSubmitDepts"
    >
      <div class="max-h-96 overflow-auto">
        <NTree
          :data="deptTree"
          checkable
          :checked-keys="deptForm.checkedKeys"
          @update:checked-keys="handleDeptCheck"
          default-expand-all
          :expand-on-click="false"
        />
      </div>
    </WwModal>
  </div>
</template>
