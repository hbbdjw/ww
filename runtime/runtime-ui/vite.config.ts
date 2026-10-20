import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import tailwindcss from '@tailwindcss/vite'
import Icons from 'unplugin-icons/vite'
import { resolve } from 'path'

// runtime server 地址（本地 node-0：REST 8090；docker node1-4 为 8091-8094）
// 用 localhost 而非 127.0.0.1：本机 localhost 优先解析到 ::1，双栈均可正常到达 node-0
const RUNTIME_SERVER = process.env.RUNTIME_SERVER ?? 'http://localhost:8090'

export default defineConfig({
  plugins: [
    vue(),
    tailwindcss(),
    // vscode-icons 构建时编译为 Vue 组件（完全离线，无运行时 API 请求）
    Icons({ compiler: 'vue3' })
  ],
  base: './',
  resolve: {
    alias: {
      '@': resolve(__dirname, 'src')
    }
  },
  build: {
    outDir: resolve(__dirname, '../server/src/main/resources/static'),
    emptyOutDir: true,
    assetsDir: 'assets',
    sourcemap: false,
    minify: 'esbuild'
  },
  server: {
    // 显式绑 IPv4：默认 localhost 在本机仅解析到 ::1，IPv4-only 客户端访问会超时
    host: '127.0.0.1',
    port: 3000,
    proxy: {
      '/api': {
        target: RUNTIME_SERVER,
        changeOrigin: true
      },
      '/rt': {
        target: RUNTIME_SERVER,
        changeOrigin: true
      },
      '/cache': {
        target: RUNTIME_SERVER,
        changeOrigin: true
      },
      '/storage': {
        target: RUNTIME_SERVER,
        changeOrigin: true
      }
    }
  }
})
