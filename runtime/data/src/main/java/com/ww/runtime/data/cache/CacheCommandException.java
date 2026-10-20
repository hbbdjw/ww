package com.ww.runtime.data.cache;

/** 命令语义错误（参数错误、未知命令、跨分区等），由协议层映射为对应状态码。 */
public final class CacheCommandException extends RuntimeException {

    public static final String UNKNOWN_COMMAND = "UNKNOWN_COMMAND";
    public static final String INVALID_ARGUMENT = "INVALID_ARGUMENT";
    public static final String WRONGTYPE = "WRONGTYPE";
    public static final String CROSSSLOT = "CROSSSLOT";
    public static final String COMMAND_FAILED = "COMMAND_FAILED";

    private final String code;

    public CacheCommandException(String code, String message) {
        super(message);
        this.code = code;
    }

    public String code() {
        return code;
    }
}
