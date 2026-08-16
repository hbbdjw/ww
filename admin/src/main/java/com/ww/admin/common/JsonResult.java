package com.ww.admin.common;

import lombok.Getter;
import lombok.Setter;

/**
 *
 *
 */
@Setter
@Getter
public class JsonResult {

    private Type type;
    private Object data;
    private String code;
    private String msg;

    /**
     * 构造方法
     *
     * @param type 类型
     * @param data 数据
     */
    public JsonResult(Type type, Object data, String code) {
        this.type = type;
        this.data = data;
        this.code = code;
    }

    /**
     * 构造方法
     *
     * @param type 类型
     * @param msg  消息
     */
    public JsonResult(Type type, String msg, String code) {
        this.type = type;
        this.msg = msg;
        this.code = code;
    }

    /**
     * 构造方法
     *
     * @param type 类型
     * @param msg  消息
     * @param data 数据
     */
    public JsonResult(Type type, Object data, String msg, String code) {
        this.type = type;
        this.data = data;
        this.msg = msg;
        this.code = code;
    }

    public static JsonResult success(Object data) {
        return new JsonResult(Type.success, data, "0");
    }

    public static JsonResult success(String msg) {
        return new JsonResult(Type.success, msg, "0");
    }

    public static JsonResult success(Object data, String msg) {
        return new JsonResult(Type.success, data, msg, "0");
    }

    public static JsonResult error(Object data) {
        return new JsonResult(Type.error, data, "2");
    }

    public static JsonResult error(String msg) {
        return new JsonResult(Type.error, msg, "2");
    }

    public static JsonResult error(Object data, String msg) {
        return new JsonResult(Type.error, data, msg, "2");
    }

    public static JsonResult warn(String msg) {
        return new JsonResult(Type.warn, msg, "1");
    }

    public static JsonResult warn(Object data) {
        return new JsonResult(Type.warn, data, "1");
    }

    public static JsonResult warn(Object data, String msg) {
        return new JsonResult(Type.warn, data, msg, "1");
    }

    /**
     * 类型
     */
    public enum Type {

        /**
         * 成功
         */
        success,

        /**
         * 警告
         */
        warn,

        /**
         * 错误
         */
        error
    }

}
