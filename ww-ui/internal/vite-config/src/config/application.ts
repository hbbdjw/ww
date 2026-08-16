import type { CSSOptions, UserConfig } from 'vite';

import type { DefineApplicationOptions } from '../typing';

import path, { relative } from 'node:path';

import { NodePackageImporter } from 'sass-embedded';
import { defineConfig, loadEnv, mergeConfig } from 'vite';

import { defaultImportmapOptions, getDefaultPwaOptions } from '../options';
import { loadApplicationPlugins } from '../plugins';
import { loadAndConvertEnv } from '../utils/env';
import { getCommonConfig } from './common';

function defineApplicationConfig(userConfigPromise?: DefineApplicationOptions) {
  return defineConfig(async (config) => {
    const options = await userConfigPromise?.(config);
    const { appTitle, base, port, ...envConfig } = await loadAndConvertEnv();
    const { command, mode } = config;
    const { application = {}, vite = {} } = options || {};
    const root = process.cwd();
    const isBuild = command === 'build';
    const env = loadEnv(mode, root);

    const plugins = await loadApplicationPlugins({
      archiver: true,
      archiverPluginOptions: {},
      compress: false,
      compressTypes: ['brotli', 'gzip'],
      devtools: true,
      env,
      extraAppConfig: true,
      html: true,
      i18n: true,
      importmapOptions: defaultImportmapOptions,
      injectAppLoading: true,
      injectMetadata: true,
      isBuild,
      license: true,
      mode,
      print: !isBuild,
      printInfoMap: {
        'Vben Admin Docs': 'https://doc.vben.pro',
      },
      pwa: true,
      pwaOptions: getDefaultPwaOptions(appTitle),
      vxeTableLazyImport: true,
      ...envConfig,
      ...application,
    });

    const { injectGlobalScss = true } = application;

    const applicationConfig: UserConfig = {
      base,
      build: {
        rolldownOptions: {
          output: {
            // 按扩展名分目录存放资产；无扩展名的资产（如 LICENSE）
            // 单独放到 assets 目录，避免生成以 / 开头的非法输出路径
            assetFileNames: (assetInfo: { names?: string[] }) => {
              const name = assetInfo.names?.[0] ?? '';
              const ext = name.includes('.') ? name.split('.').pop() : '';
              return ext
                ? `${ext}/[name]-[hash].[ext]`
                : 'assets/[name]-[hash]';
            },
            chunkFileNames: 'js/[name]-[hash].js',
            entryFileNames: 'jse/index-[name]-[hash].js',
            // 版权注释保留在产物内，避免抽取出额外的 LICENSE 资产文件
            legalComments: 'inline',
            minify: isBuild
              ? {
                  compress: {
                    dropDebugger: true,
                  },
                }
              : false,
          },
        },
        target: 'es2015',
      },
      css: createCssOptions(injectGlobalScss),
      plugins,
      server: {
        host: true,
        port,
        warmup: {
          // 预热文件
          clientFiles: [
            './index.html',
            './src/bootstrap.ts',
            './src/{views,layouts,router,store,api,adapter}/*',
          ],
        },
      },
    };

    const mergedCommonConfig = mergeConfig(
      await getCommonConfig(),
      applicationConfig,
    );
    return mergeConfig(mergedCommonConfig, vite);
  });
}

function createCssOptions(injectGlobalScss = true): CSSOptions {
  const root = process.cwd();
  return {
    preprocessorOptions: injectGlobalScss
      ? {
          scss: {
            additionalData: (content: string, filepath: string) => {
              const relativePath = relative(root, filepath);
              // 应用源码（src 目录）注入全局样式
              if (relativePath.startsWith(`src${path.sep}`)) {
                return `@use "@vben/styles/global" as *;\n${content}`;
              }
              return content;
            },
            // api: 'modern',
            importers: [new NodePackageImporter()],
          },
        }
      : {},
  };
}

export { defineApplicationConfig };
