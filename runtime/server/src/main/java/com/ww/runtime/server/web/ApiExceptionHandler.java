package com.ww.runtime.server.web;

import java.util.Map;

import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.DisabledException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.async.AsyncRequestNotUsableException;
import org.springframework.web.servlet.resource.NoResourceFoundException;

import com.ww.runtime.common.RuntimeFrameworkException;

/**
 * API 全局异常处理：统一错误体 {@code {"message": ...}} 与状态码映射。
 *
 * <ul>
 *   <li>非法参数 / 前置校验失败 → 400；</li>
 *   <li>凭证错误 → 401；账号停用 / 权限不足 → 403；</li>
 *   <li>缺失静态资源（favicon 等常态噪音）→ 404，仅记关键信息不打堆栈；</li>
 *   <li>SSE/流式响应期间客户端断开（{@link AsyncRequestNotUsableException}）→ 连接已不可写，
 *       静默消化，仅 DEBUG 留痕；</li>
 *   <li>框架能力异常（如会话缓存未就绪）→ 503；其余未预期异常 → 500（记完整堆栈）。</li>
 * </ul>
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> illegalArgument(IllegalArgumentException e) {
        return badRequest(e.getMessage());
    }

    /** 请求体缺失或 JSON 格式错误 → 400（客户端输入问题，非服务端故障）。 */
    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<Map<String, Object>> unreadable(HttpMessageNotReadableException e) {
        return badRequest("请求体格式不合法，请检查 JSON 内容");
    }

    @ExceptionHandler(BadCredentialsException.class)
    public ResponseEntity<Map<String, Object>> badCredentials(BadCredentialsException e) {
        return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
                .body(Map.of("message", nullToDefault(e.getMessage(), "用户名或密码错误")));
    }

    @ExceptionHandler(DisabledException.class)
    public ResponseEntity<Map<String, Object>> disabled(DisabledException e) {
        return ResponseEntity.status(HttpStatus.FORBIDDEN)
                .body(Map.of("message", nullToDefault(e.getMessage(), "账号已停用")));
    }

    @ExceptionHandler(AccessDeniedException.class)
    public ResponseEntity<Map<String, Object>> accessDenied(AccessDeniedException e) {
        return ResponseEntity.status(HttpStatus.FORBIDDEN)
                .body(Map.of("message", "没有访问权限"));
    }

    @ExceptionHandler(RuntimeFrameworkException.class)
    public ResponseEntity<Map<String, Object>> framework(RuntimeFrameworkException e) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(Map.of("message", nullToDefault(e.getMessage(), "服务暂不可用")));
    }

    /**
     * 缺失静态资源（如 favicon.ico）/ 未命中任何 Controller 映射 → 404。
     *
     * <p>这类请求是常态噪音（浏览器自动请求 favicon、误探路径），不是服务端故障：
     * 只记录一条关键信息日志，不打印堆栈，避免刷屏。</p>
     */
    @ExceptionHandler(NoResourceFoundException.class)
    public ResponseEntity<Map<String, Object>> noResource(NoResourceFoundException e) {
        LOG.info("no static resource: {}", e.getMessage());
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(Map.of("message", e.getMessage()));
    }

    /**
     * 异步响应写出阶段客户端已断开：SSE 长连接（集群状态/实时日志推送）在浏览器关闭页面、
     * 路由跳转、网络切换或中间代理超时时必然发生，属长连接常态事件而非服务端故障。
     *
     * <p>推送线程自身的 try/catch 会清理失效订阅；容器随后还会经 ASYNC error dispatch
     * 将本异常重新抛回 DispatcherServlet，此处显式兜底，避免落入通用 500 分支刷 ERROR 堆栈。
     * 连接已经不可写，不返回错误体（void 即视为已处理），仅 DEBUG 留痕。</p>
     */
    @ExceptionHandler(AsyncRequestNotUsableException.class)
    public void asyncRequestNotUsable(AsyncRequestNotUsableException e, HttpServletRequest request) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("client disconnected during async response: {} {} ({})",
                    request.getMethod(), request.getRequestURI(), e.getMessage());
        }
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> unexpected(Exception e) {
        // 未预期异常必须记录完整堆栈（响应体不向客户端泄漏内部细节）
        LOG.error("unexpected API error", e);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("message", "服务器内部错误"));
    }

    private static ResponseEntity<Map<String, Object>> badRequest(String message) {
        return ResponseEntity.badRequest()
                .body(Map.of("message", nullToDefault(message, "请求参数不合法")));
    }

    private static String nullToDefault(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value;
    }
}
