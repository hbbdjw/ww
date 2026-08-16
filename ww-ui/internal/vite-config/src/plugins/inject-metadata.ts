import type { PluginOption } from 'vite';

import { dateUtil, readPackageJSON } from '../../../node-utils/src/index';

/**
 * 用于注入项目信息
 */
async function viteMetadataPlugin(
  root = process.cwd(),
): Promise<PluginOption | undefined> {
  const {
    author,
    dependencies = {},
    description,
    devDependencies = {},
    homepage,
    license,
    version,
  } = await readPackageJSON(root);

  const buildTime = dateUtil().format('YYYY-MM-DD HH:mm:ss');

  return {
    async config() {
      const isAuthorObject = typeof author === 'object';
      const authorName = isAuthorObject ? author.name : author;
      const authorEmail = isAuthorObject ? author.email : null;
      const authorUrl = isAuthorObject ? author.url : null;

      return {
        define: {
          __VBEN_ADMIN_METADATA__: JSON.stringify({
            authorEmail,
            authorName,
            authorUrl,
            buildTime,
            dependencies,
            description,
            devDependencies,
            homepage,
            license,
            version,
          }),
          'import.meta.env.VITE_APP_VERSION': JSON.stringify(version),
        },
      };
    },
    enforce: 'post',
    name: 'vite:inject-metadata',
  };
}

export { viteMetadataPlugin };
