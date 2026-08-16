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
  deleteDeptApi,
  getDeptListApi,
  saveDeptApi,
  type SysDept,
} from '#/api/core/sys-dept';
import { WwModal } from '#/components/WwModal';

defineOptions({ name: 'SystemDept' });

const loading = ref(false);
const depts = ref<SysDept[]>([]);

const showFormModal = ref(false);
const isEdit = ref(false);

const formData = reactive<{
  deptName: string;
  id: null | number;
  orderNum: number;
  parentId: number;
  status: number;
}>({
  id: null,
  deptName: '',
  parentId: 0,
  orderNum: 0,
  status: 1,
});

const formRef = ref();

const formRules: FormRules = {
  deptName: [
    { required: true, message: '请输入部门名称', trigger: 'blur' },
    { max: 50, message: '部门名称最长50位', trigger: 'blur' },
  ],
};

function buildTree(list: SysDept[], parentId = 0): (SysDept & { children?: SysDept[] })[] {
  const map = new Map<number, SysDept & { children?: SysDept[] }>();
  for (const d of list) {
    map.set(d.id, { ...d, children: [] });
  }
  const roots: (SysDept & { children?: SysDept[] })[] = [];
  for (const d of map.values()) {
    if (d.parentId === 0 || d.parentId === parentId) {
      roots.push(d);
    } else {
      const parent = map.get(d.parentId);
      if (parent) {
        parent.children!.push(d);
      } else {
        roots.push(d);
      }
    }
  }
  return roots;
}

const deptTree = computed(() => buildTree(depts.value));

const parentOptions = computed(() => {
  const opts: { label: string; value: number }[] = [
    { label: '顶级部门', value: 0 },
  ];
  for (const d of depts.value) {
    opts.push({ label: d.deptName, value: d.id });
  }
  return opts;
});

const columns: DataTableColumns<SysDept> = [
  {
    title: '部门名称',
    key: 'deptName',
    width: 200,
  },
  {
    title: '排序',
    key: 'orderNum',
    width: 80,
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
    width: 200,
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
            onPositiveClick: () => handleDelete(row.id),
          },
          {
            trigger: () =>
              h(
                NButton,
                { size: 'small', type: 'error', quaternary: true },
                { default: () => '删除' },
              ),
            default: () => `确定删除部门 ${row.deptName} 吗？`,
          },
        ),
      ]);
    },
  },
];

async function fetchData() {
  loading.value = true;
  try {
    depts.value = await getDeptListApi();
  } finally {
    loading.value = false;
  }
}

function handleAdd() {
  isEdit.value = false;
  Object.assign(formData, {
    id: null,
    deptName: '',
    parentId: 0,
    orderNum: 0,
    status: 1,
  });
  showFormModal.value = true;
}

function handleAddChild(row: SysDept) {
  isEdit.value = false;
  Object.assign(formData, {
    id: null,
    deptName: '',
    parentId: row.id,
    orderNum: 0,
    status: 1,
  });
  showFormModal.value = true;
}

function handleEdit(row: SysDept) {
  isEdit.value = true;
  Object.assign(formData, {
    id: row.id,
    deptName: row.deptName,
    parentId: row.parentId || 0,
    orderNum: row.orderNum || 0,
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
  await saveDeptApi({
    id: formData.id ?? undefined,
    deptName: formData.deptName,
    parentId: formData.parentId,
    orderNum: formData.orderNum,
    status: formData.status,
  });
  message.success(isEdit.value ? '更新成功' : '创建成功');
  showFormModal.value = false;
  await fetchData();
}

async function handleDelete(id: number) {
  await deleteDeptApi(id);
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
        :data="deptTree"
        :loading="loading"
        bordered
        :scroll-x="700"
        :single-line="false"
        default-expand-all
        :row-key="(row: SysDept) => row.id"
        children-key="children"
      />
    </NCard>

    <!-- 新增/编辑部门弹窗 -->
    <WwModal
      v-model:show="showFormModal"
      :title="isEdit ? '编辑部门' : '新增部门'"
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
        <NFormItem label="上级部门">
          <NSelect
            v-model:value="formData.parentId"
            :options="parentOptions"
            placeholder="请选择上级部门"
            filterable
          />
        </NFormItem>
        <NFormItem label="部门名称" path="deptName">
          <NInput v-model:value="formData.deptName" placeholder="请输入部门名称" />
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
