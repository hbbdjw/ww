import type { RouteRecordRaw } from 'vue-router';

const routes: RouteRecordRaw[] = [
  {
    meta: {
      icon: 'lucide:app-window',
      order: 10,
      title: '组件示例',
    },
    name: 'Demos',
    path: '/demos',
    children: [
      {
        name: 'WwModalDemo',
        path: 'modal',
        component: () => import('#/views/demos/naive/modal/index.vue'),
        meta: {
          icon: 'lucide:panel-top',
          title: '通用弹出层',
        },
      },
    ],
  },
];

export default routes;
