package com.ww.runtime.lb;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * 反向代理响应：状态码、响应头（多值）、流式响应体（可能为空流）。
 * try-with-resources 关闭即关闭底层连接流。
 */
public record ForwardResponse(
        int statusCode,
        Map<String, List<String>> headers,
        InputStream body) implements AutoCloseable {

    @Override
    public void close() throws java.io.IOException {
        if (body != null) {
            body.close();
        }
    }
}
