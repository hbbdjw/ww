import { getAliases } from './alias';
import { defineConfig } from './internal/vite-config/src';

// 轮询索引（每次请求切换到下一个节点）

export default defineConfig(async () => {
  return {
    application: {},
    vite: {
      resolve: {
        // 原 monorepo 内部包统一通过别名解析到源码
        alias: getAliases(),
      },
      server: {
        proxy: {
          '/api': {
            // 默认 target（router 返回值会覆盖此值，此处作为兜底）
            target: 'http://localhost:7777',
            changeOrigin: true,
            rewrite: (path) => path.replace(/^\/api/, ''),
            ws: true
          },
        },
      },
    },
  };
});
