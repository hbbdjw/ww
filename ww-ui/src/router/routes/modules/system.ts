import type { RouteRecordRaw } from 'vue-router';

const routes: RouteRecordRaw[] = [
  {
    meta: {
      icon: 'lucide:settings',
      order: 10,
      title: '系统管理',
    },
    name: 'System',
    path: '/system',
    children: [
      {
        name: 'SystemUser',
        path: 'user',
        component: () => import('#/views/system/user/index.vue'),
        meta: {
          icon: 'lucide:users',
          title: '用户管理',
        },
      },
      {
        name: 'SystemDept',
        path: 'dept',
        component: () => import('#/views/system/dept/index.vue'),
        meta: {
          icon: 'lucide:git-fork',
          title: '部门管理',
        },
      },
      {
        name: 'SystemRole',
        path: 'role',
        component: () => import('#/views/system/role/index.vue'),
        meta: {
          icon: 'lucide:shield-check',
          title: '角色管理',
        },
      },
      {
        name: 'SystemMenu',
        path: 'menu',
        component: () => import('#/views/system/menu/index.vue'),
        meta: {
          icon: 'lucide:menu',
          title: '权限管理',
        },
      },
      {
        name: 'SystemCluster',
        path: 'cluster',
        component: () => import('#/views/system/cluster/index.vue'),
        meta: {
          icon: 'lucide:network',
          title: '集群管理',
        },
      },
      {
        name: 'SystemStorage',
        path: 'storage',
        component: () => import('#/views/system/storage/index.vue'),
        meta: {
          icon: 'lucide:hard-drive',
          title: '存储管理',
        },
      },
      {
        name: 'SystemCache',
        path: 'cache',
        component: () => import('#/views/system/cache/index.vue'),
        meta: {
          icon: 'lucide:database',
          title: '缓存管理',
        }
      },
      {
        name: 'SystemLog',
        path: 'log',
        component: () => import('#/views/system/log/index.vue'),
        meta: {
          icon: 'lucide:scroll-text',
          title: '日志管理',
        },
      },
      {
        name: 'Test',
        path: 'test',
        component: () => import('#/views/system/test/index.vue'),
        meta: {
          icon: 'lucide:database',
          title: '测试',
        },
      },
    ],
  },
];

export default routes;
