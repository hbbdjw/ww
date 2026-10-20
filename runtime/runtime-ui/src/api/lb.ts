import { get, put } from './request'

/** 分发策略：轮询 / 平滑权重 / 随机 */
export type LbStrategyName = 'ROUND_ROBIN' | 'WEIGHTED' | 'RANDOM'

/** GET/PUT /rt/lb/config 响应体（与后端 com.ww.runtime.lb.LbConfig 对齐） */
export interface LbConfig {
  /** 总开关：false=上游已有 LBS/Nginx，过滤器直通 */
  enabled: boolean
  /** 唯一流量入口节点 id；enabled=true 时必填 */
  entryNodeId: string | null
  strategy: LbStrategyName
  /** 入口节点自身是否参与处理部分请求 */
  selfServe: boolean
  /** 代理路径前缀（Spring PathPattern 语法），默认 /cache/**、/storage/** */
  pathPrefixes: string[]
  /** 节点 LB 权重覆盖；缺键=用节点注册权重 */
  weightOverrides: Record<string, number>
  configRefreshMs: number
  nodeViewRefreshMs: number
  connectTimeoutMs: number
  requestTimeoutMs: number
  /** 乐观锁版本，每次成功更新 +1 */
  version: number
}

/** 读取集群级负载均衡配置 */
export async function getLbConfigApi(): Promise<LbConfig> {
  return get<LbConfig>('/rt/lb/config')
}

/** 更新配置（携带 version CAS；版本冲突时后端返回 409） */
export async function updateLbConfigApi(config: LbConfig): Promise<LbConfig> {
  return put<LbConfig>('/rt/lb/config', config)
}
