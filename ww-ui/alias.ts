import path from 'node:path';

const root = import.meta.dirname;

/**
 * 原 monorepo 内部包 -> 源码入口文件的映射。
 * 去除 workspace 后，通过 vite alias / tsconfig paths 直接解析到源码，
 * 不再依赖 pnpm workspace 链接。
 */
const sourceAliases: Record<string, string> = {
  '@vben-core/composables': 'packages/@core/composables/src/index.ts',
  '@vben-core/design': 'packages/@core/base/design/src/index.ts',
  '@vben-core/design/bem': 'packages/@core/base/design/src/scss-bem/bem.scss',
  '@vben-core/design/theme': 'packages/@core/base/design/src/css/global.css',
  '@vben-core/form-ui': 'packages/@core/ui-kit/form-ui/src/index.ts',
  '@vben-core/icons': 'packages/@core/base/icons/src/index.ts',
  '@vben-core/layout-ui': 'packages/@core/ui-kit/layout-ui/src/index.ts',
  '@vben-core/menu-ui': 'packages/@core/ui-kit/menu-ui/src/index.ts',
  '@vben-core/popup-ui': 'packages/@core/ui-kit/popup-ui/src/index.ts',
  '@vben-core/preferences': 'packages/@core/preferences/src/index.ts',
  '@vben-core/shadcn-ui': 'packages/@core/ui-kit/shadcn-ui/src/index.ts',
  '@vben-core/shared/cache': 'packages/@core/base/shared/src/cache/index.ts',
  '@vben-core/shared/color': 'packages/@core/base/shared/src/color/index.ts',
  '@vben-core/shared/constants':
    'packages/@core/base/shared/src/constants/index.ts',
  '@vben-core/shared/global-state':
    'packages/@core/base/shared/src/global-state.ts',
  '@vben-core/shared/store': 'packages/@core/base/shared/src/store.ts',
  '@vben-core/shared/utils': 'packages/@core/base/shared/src/utils/index.ts',
  '@vben-core/tabs-ui': 'packages/@core/ui-kit/tabs-ui/src/index.ts',
  '@vben-core/typings': 'packages/@core/base/typings/src/index.ts',
  '@vben/access': 'packages/effects/access/src/index.ts',
  '@vben/common-ui': 'packages/effects/common-ui/src/index.ts',
  '@vben/common-ui/es/loading':
    'packages/effects/common-ui/src/components/loading/index.ts',
  '@vben/common-ui/es/tippy':
    'packages/effects/common-ui/src/components/tippy/index.ts',
  '@vben/constants': 'packages/constants/src/index.ts',
  '@vben/hooks': 'packages/effects/hooks/src/index.ts',
  '@vben/icons': 'packages/icons/src/index.ts',
  '@vben/layouts': 'packages/effects/layouts/src/index.ts',
  '@vben/locales': 'packages/locales/src/index.ts',
  '@vben/plugins': 'packages/effects/plugins/src/index.ts',
  '@vben/plugins/echarts': 'packages/effects/plugins/src/echarts/index.ts',
  '@vben/plugins/motion': 'packages/effects/plugins/src/motion/index.ts',
  '@vben/plugins/vxe-table': 'packages/effects/plugins/src/vxe-table/index.ts',
  '@vben/preferences': 'packages/preferences/src/index.ts',
  '@vben/request': 'packages/effects/request/src/index.ts',
  '@vben/stores': 'packages/stores/src/index.ts',
  '@vben/styles': 'packages/styles/src/index.ts',
  '@vben/styles/global': 'packages/styles/src/global/index.scss',
  '@vben/styles/naive': 'packages/styles/src/naive/index.css',
  '@vben/tailwind-config/theme': 'internal/tailwind-config/src/theme.css',
  '@vben/types': 'packages/types/src/index.ts',
  '@vben/utils': 'packages/utils/src/index.ts',
};

/**
 * 生成精确匹配的 vite resolve.alias 列表
 */
function getAliases() {
  const aliases = Object.entries(sourceAliases).map(([find, target]) => ({
    find: new RegExp(`^${find.replaceAll('/', '\\/')}$`),
    replacement: path.resolve(root, target),
  }));
  // package.json "imports" 中的 #/ 子路径别名
  aliases.push({
    find: /^#\//,
    replacement: `${path.resolve(root, 'src')}/`,
  });
  return aliases;
}

export { getAliases };
