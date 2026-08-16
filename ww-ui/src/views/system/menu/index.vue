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
  NInputNumber,
  NPopconfirm,
  NSelect,
  NSpace,
  NSwitch,
  NTag,
} from 'naive-ui';

import { message } from '#/adapter/naive';
import {
  deleteMenuApi,
  getMenuListApi,
  saveMenuApi,
  type SysMenu,
} from '#/api/core/sys-menu';
import { WwModal } from '#/components/WwModal';

defineOptions({ name: 'SystemMenu' });

const loading = ref(false);
const menus = ref<SysMenu[]>([]);

const showFormModal = ref(false);
const isEdit = ref(false);

const menuTypeOptions = [
  { label: '目录', value: 'M' },
  { label: '菜单', value: 'C' },
  { label: '按钮', value: 'F' },
];

const formData = reactive<{
  component: string;
  id: null | number;
  menuName: string;
  menuType: string;
  orderNum: number;
  parentId: number;
  path: string;
  perms: string;
  status: number;
}>({
  id: null,
  menuName: '',
  parentId: 0,
  orderNum: 0,
  path: '',
  component: '',
  menuType: 'C',
  perms: '',
  status: 1,
});

const formRef = ref();

const formRules: FormRules = {
  menuName: [
    { required: true, message: '请输入菜单名称', trigger: 'blur' },
    { max: 50, message: '菜单名称最长50位', trigger: 'blur' },
  ],
  menuType: [
    { required: true, message: '请选择菜单类型', trigger: 'change' },
  ],
};

function buildTree(list: SysMenu[], parentId = 0): (SysMenu & { children?: SysMenu[] })[] {
  const map = new Map<number, SysMenu & { children?: SysMenu[] }>();
  for (const m of list) {
    map.set(m.id, { ...m, children: [] });
  }
  const roots: (SysMenu & { children?: SysMenu[] })[] = [];
  for (const m of map.values()) {
    if (!m.parentId || m.parentId === 0 || m.parentId === parentId) {
      roots.push(m);
    } else {
      const parent = map.get(m.parentId);
      if (parent) {
        parent.children!.push(m);
      } else {
        roots.push(m);
      }
    }
  }
  return roots;
}

const menuTree = computed(() => buildTree(menus.value));

const parentOptions = computed(() => {
  const opts: { label: string; value: number }[] = [
    { label: '顶级目录', value: 0 },
  ];
  for (const m of menus.value) {
    if (m.menuType !== 'F') {
      opts.push({ label: m.menuName, value: m.id });
    }
  }
  return opts;
});

const isButton = computed(() => formData.menuType === 'F');

const columns: DataTableColumns<SysMenu> = [
  { title: '菜单名称', key: 'menuName', width: 180 },
  {
    title: '类型',
    key: 'menuType',
    width: 80,
    render(row) {
      const typeMap: Record<string, { label: string; type: 'default' | 'error' | 'info' | 'success' | 'warning' }> = {
        M: { label: '目录', type: 'warning' },
        C: { label: '菜单', type: 'success' },
        F: { label: '按钮', type: 'info' },
      };
      const t = typeMap[row.menuType] || { label: '未知', type: 'default' };
      return h(NTag, { size: 'small', type: t.type }, { default: () => t.label });
    },
  },
  {
    title: '路由路径',
    key: 'path',
    width: 150,
    render(row) {
      return h('span', { class: 'text-xs text-gray-500 font-mono' }, row.path || '-');
    },
  },
  {
    title: '组件',
    key: 'component',
    width: 180,
    render(row) {
      return h('span', { class: 'text-xs text-gray-500 font-mono' }, row.component || '-');
    },
  },
  {
    title: '权限标识',
    key: 'perms',
    width: 180,
    render(row) {
      return h('span', { class: 'text-xs text-gray-500 font-mono' }, row.perms || '-');
    },
  },
  { title: '排序', key: 'orderNum', width: 60 },
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
  { title: '创建时间', key: 'createdDate', width: 160 },
  {
    title: '操作',
    key: 'actions',
    width: 240,
    fixed: 'right',
    render(row) {
      return h(NSpace, { size: 'small' }, () => [
        h(
          NButton,
          {
            size: 'small',
            type: 'primary',
            quaternary: true,
            onClick: () => handleAddChild(row),
            disabled: row.menuType === 'F',
          },
          { default: () => '新增' },
        ),
        h(
          NButton,
          {
            size: 'small',
            type: 'info',
            quaternary: true,
            onClick: () => handleEdit(row),
          },
          { default: () => '编辑' },
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
            default: () => `确定删除菜单 ${row.menuName} 吗？`,
          },
        ),
      ]);
    },
  },
];

async function fetchData() {
  loading.value = true;
  try {
    menus.value = await getMenuListApi();
  } finally {
    loading.value = false;
  }
}

function handleAdd() {
  isEdit.value = false;
  Object.assign(formData, {
    id: null,
    menuName: '',
    parentId: 0,
    orderNum: 0,
    path: '',
    component: '',
    menuType: 'C',
    perms: '',
    status: 1,
  });
  showFormModal.value = true;
}

function handleAddChild(row: SysMenu) {
  isEdit.value = false;
  Object.assign(formData, {
    id: null,
    menuName: '',
    parentId: row.id,
    orderNum: 0,
    path: '',
    component: '',
    menuType: 'C',
    perms: '',
    status: 1,
  });
  showFormModal.value = true;
}

function handleEdit(row: SysMenu) {
  isEdit.value = true;
  Object.assign(formData, {
    id: row.id,
    menuName: row.menuName,
    parentId: row.parentId || 0,
    orderNum: row.orderNum || 0,
    path: row.path || '',
    component: row.component || '',
    menuType: row.menuType || 'C',
    perms: row.perms || '',
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
  await saveMenuApi({
    id: formData.id ?? undefined,
    menuName: formData.menuName,
    parentId: formData.parentId,
    orderNum: formData.orderNum,
    path: isButton.value ? '' : formData.path,
    component: isButton.value ? '' : formData.component,
    menuType: formData.menuType,
    perms: formData.perms,
    status: formData.status,
  });
  message.success(isEdit.value ? '更新成功' : '创建成功');
  showFormModal.value = false;
  await fetchData();
}

async function handleDelete(id: number) {
  await deleteMenuApi(id);
  message.success('删除成功');
  await fetchData();
}

onMounted(() => {
  fetchData();
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

      <!-- 数据表格（树形） -->
      <NDataTable
        :columns="columns"
        :data="menuTree"
        :loading="loading"
        bordered
        :scroll-x="1200"
        :single-line="false"
        default-expand-all
        :row-key="(row: SysMenu) => row.id"
        children-key="children"
      />
    </NCard>

    <!-- 新增/编辑菜单弹窗 -->
    <WwModal
      v-model:show="showFormModal"
      :title="isEdit ? '编辑菜单' : '新增菜单'"
      :width="520"
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
        <NFormItem label="上级菜单">
          <NSelect
            v-model:value="formData.parentId"
            :options="parentOptions"
            placeholder="请选择上级菜单"
            filterable
          />
        </NFormItem>
        <NFormItem label="菜单类型" path="menuType">
          <NSelect
            v-model:value="formData.menuType"
            :options="menuTypeOptions"
            placeholder="请选择菜单类型"
          />
        </NFormItem>
        <NFormItem label="菜单名称" path="menuName">
          <NInput v-model:value="formData.menuName" placeholder="请输入菜单名称" />
        </NFormItem>
        <NFormItem v-if="!isButton" label="路由路径">
          <NInput v-model:value="formData.path" placeholder="如 /system/user" />
        </NFormItem>
        <NFormItem v-if="!isButton" label="组件路径">
          <NInput v-model:value="formData.component" placeholder="如 system/user/index" />
        </NFormItem>
        <NFormItem label="权限标识">
          <NInput v-model:value="formData.perms" placeholder="如 sys:user:list" />
        </NFormItem>
        <NFormItem label="排序">
          <NInputNumber
            v-model:value="formData.orderNum"
            :min="0"
            placeholder="排序号"
            style="width: 100%"
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
  </div>
</template>
