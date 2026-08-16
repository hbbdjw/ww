import * as monaco from 'monaco-editor';
import editorWorker from 'monaco-editor/editor/editor.worker?worker';
import jsonWorker from 'monaco-editor/language/json/json.worker?worker';
import tsWorker from 'monaco-editor/language/typescript/ts.worker?worker';

/**
 * Monaco worker setup (Vite native workers, no CDN).
 * json/ts language services need dedicated workers for validation and IntelliSense.
 */
self.MonacoEnvironment = {
  getWorker(_workerId: string, label: string) {
    switch (label) {
      case 'javascript':
      case 'typescript': {
        return new tsWorker();
      }
      case 'json': {
        return new jsonWorker();
      }
      default: {
        return new editorWorker();
      }
    }
  },
};

export default monaco;
