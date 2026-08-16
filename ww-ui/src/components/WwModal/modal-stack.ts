/**
 * 多弹窗层级栈管理
 *
 * 负责管理同时存在的多个弹窗的 zIndex 分配：
 * - 新打开的弹窗始终位于已有弹窗之上；
 * - 用户操作（点击/聚焦）某个弹窗时，自动将其提升到最顶层；
 * - 最小化的弹窗不参与"最顶层"判断（ESC 只关闭最顶层未最小化弹窗）；
 * - 所有弹窗关闭后重置层级基准，避免 zIndex 无限增长。
 */

const BASE_Z_INDEX = 2000;
const Z_INDEX_STEP = 10;

interface ModalEntry {
  id: symbol;
  minimized: boolean;
  zIndex: number;
}

const stack: ModalEntry[] = [];
let topZIndex = BASE_Z_INDEX - Z_INDEX_STEP;

/**
 * 注册一个弹窗，返回为其分配的 zIndex
 * @param id 弹窗唯一标识
 * @param zIndex 期望的基础层级（可选），实际层级不会低于该值
 */
function register(id: symbol, zIndex?: number): number {
  topZIndex = Math.max(topZIndex, zIndex ?? BASE_Z_INDEX - Z_INDEX_STEP);
  topZIndex += Z_INDEX_STEP;
  stack.push({ id, minimized: false, zIndex: topZIndex });
  return topZIndex;
}

/**
 * 将弹窗提升到最顶层（用户操作触发的层级自动切换），返回新的 zIndex
 */
function toTop(id: symbol): number {
  const entry = stack.find((item) => item.id === id);
  if (!entry) {
    return topZIndex;
  }
  topZIndex += Z_INDEX_STEP;
  entry.zIndex = topZIndex;
  return topZIndex;
}

/**
 * 注销弹窗；全部注销后重置层级基准
 */
function unregister(id: symbol): void {
  const index = stack.findIndex((item) => item.id === id);
  if (index > -1) {
    stack.splice(index, 1);
  }
  if (stack.length === 0) {
    topZIndex = BASE_Z_INDEX - Z_INDEX_STEP;
  }
}

/**
 * 是否为最顶层的未最小化弹窗（用于 ESC 关闭判断）
 */
function isTop(id: symbol): boolean {
  let top: ModalEntry | undefined;
  for (const entry of stack) {
    if (!entry.minimized && (!top || entry.zIndex > top.zIndex)) {
      top = entry;
    }
  }
  return top?.id === id;
}

/**
 * 设置弹窗的最小化状态
 */
function setMinimized(id: symbol, minimized: boolean): void {
  const entry = stack.find((item) => item.id === id);
  if (entry) {
    entry.minimized = minimized;
  }
}

/**
 * 获取弹窗在已最小化弹窗中的序号（用于底部堆叠偏移）
 */
function minimizedIndex(id: symbol): number {
  return stack
    .filter((item) => item.minimized)
    .findIndex((item) => item.id === id);
}

export const modalStack = {
  isTop,
  minimizedIndex,
  register,
  setMinimized,
  toTop,
  unregister,
};
