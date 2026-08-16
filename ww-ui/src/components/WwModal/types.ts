import type { CSSProperties } from 'vue';

/**
 * 尺寸值：数字（px）或带单位字符串，如 '520'、'520px'、'80vw'、'80vh'、'50%'
 */
export type SizeValue = number | string;

/**
 * 缩放拖柄方向
 */
export type ResizeHandleDirection =
  | 'e'
  | 'n'
  | 'ne'
  | 'nw'
  | 's'
  | 'se'
  | 'sw'
  | 'w';

/**
 * WwModal 组件 Props
 */
export interface WwModalProps {
  /**
   * 挂载目标
   * @default 'body'
   */
  appendTo?: string;
  /**
   * 取消按钮文字
   * @default '取消'
   */
  cancelText?: string;
  /**
   * 是否垂直居中（高度自适应时通过测量居中）
   * @default true
   */
  centered?: boolean;
  /**
   * 是否显示右上角关闭按钮
   * @default true
   */
  closable?: boolean;
  /**
   * 按下 ESC 是否关闭（仅关闭最顶层弹窗）
   * @default true
   */
  closeOnEsc?: boolean;
  /**
   * 确认按钮 loading
   * @default false
   */
  confirmLoading?: boolean;
  /**
   * 确认按钮文字
   * @default '确认'
   */
  confirmText?: string;
  /**
   * 内容区域 class
   */
  contentClass?: string;
  /**
   * 打开时是否默认最大化
   * @default false
   */
  defaultMaximized?: boolean;
  /**
   * 标题下方的描述文字
   */
  description?: string;
  /**
   * 关闭时销毁内容
   * @default false
   */
  destroyOnClose?: boolean;
  /**
   * 是否可拖拽（按住头部拖动）
   * @default true
   */
  draggable?: boolean;
  /**
   * 是否显示底部按钮栏
   * @default true
   */
  footer?: boolean;
  /**
   * 底部 class
   */
  footerClass?: string;
  /**
   * 头部 class
   */
  headerClass?: string;
  /**
   * 弹窗高度，不传则内容自适应
   */
  height?: SizeValue;
  /**
   * 外链 iframe 地址，传入后内容区渲染 iframe
   */
  iframeSrc?: string;
  /**
   * 内容加载状态
   * @default false
   */
  loading?: boolean;
  /**
   * 是否显示遮罩
   * @default false
   */
  mask?: boolean;
  /**
   * 点击遮罩是否关闭
   * @default true
   */
  maskClosable?: boolean;
  /**
   * 遮罩自定义样式（如背景色、模糊等）
   */
  maskStyle?: CSSProperties;
  /**
   * 是否显示最大化/还原按钮
   * @default true
   */
  maximizable?: boolean;
  /**
   * 缩放最小高度
   * @default 120
   */
  minHeight?: number;
  /**
   * 是否显示最小化按钮
   * @default false
   */
  minimizable?: boolean;
  /**
   * 缩放最小宽度
   * @default 280
   */
  minWidth?: number;
  /**
   * 打开时相对默认位置的水平偏移（px），用于多弹窗错开摆放
   * @default 0
   */
  offsetX?: number;
  /**
   * 打开时相对默认位置的垂直偏移（px），用于多弹窗错开摆放
   * @default 0
   */
  offsetY?: number;
  /**
   * 关闭前回调，返回 false 可阻止关闭
   */
  onBeforeClose?: () => boolean | Promise<boolean | undefined | void>;
  /**
   * 是否可通过拖拽边框与四角缩放
   * @default true
   */
  resizable?: boolean;
  /**
   * 是否显示取消按钮
   * @default true
   */
  showCancelButton?: boolean;
  /**
   * 是否显示确认按钮
   * @default true
   */
  showConfirmButton?: boolean;
  /**
   * 弹窗标题
   */
  title?: string;
  /**
   * 弹出/关闭动画名（对应 *-enter-active 等过渡类名），
   * 内置：ww-modal-zoom / ww-modal-fade / ww-modal-slide-up / ww-modal-slide-down，
   * 也可传入自定义的全局过渡类名
   * @default 'ww-modal-zoom'
   */
  transitionName?: string;
  /**
   * 动画时长（毫秒），不传使用 CSS 变量默认值 250ms
   */
  duration?: number;
  /**
   * 弹窗宽度
   * @default 520
   */
  width?: SizeValue;
  /**
   * 基础层级，多弹窗共存时由弹窗栈自动管理实际 zIndex
   */
  zIndex?: number;
}
