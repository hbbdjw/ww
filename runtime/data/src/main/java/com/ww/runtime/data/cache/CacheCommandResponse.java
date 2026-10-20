package com.ww.runtime.data.cache;

/** CACHE 命令响应：成功携带结果，失败携带稳定错误码与消息。 */
public record CacheCommandResponse(boolean ok, Object result, String code, String message) {

    public static CacheCommandResponse ok(Object result) {
        return new CacheCommandResponse(true, result, null, null);
    }

    public static CacheCommandResponse error(String code, String message) {
        return new CacheCommandResponse(false, null, code, message);
    }
}
