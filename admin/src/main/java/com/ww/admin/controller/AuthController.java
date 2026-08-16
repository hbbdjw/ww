package com.ww.admin.controller;

import com.ww.admin.common.JsonResult;
import com.ww.admin.security.BearerToken;
import com.ww.admin.service.AuthService;
import jakarta.servlet.http.HttpServletRequest;
import org.apache.shiro.authc.AuthenticationException;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * 认证控制器：Session 登录、Token 签发、登出、当前用户。
 */
@RestController
@RequestMapping("/auth")
public class AuthController {

    private final AuthService authService;

    public AuthController(AuthService authService) {
        this.authService = authService;
    }

    /**
     * 登录（前端 SPA 场景）：校验用户名密码后签发 Bearer 令牌。
     * <p>请求体：{"username":"...","password":"..."}
     * <p>返回：{"accessToken":"...","tokenType":"Bearer","expiresIn":43200,"username":"..."}
     * <p>前端将 accessToken 存入 store，后续请求通过 Authorization: Bearer 头携带。
     */
    @PostMapping("/login")
    public JsonResult login(@RequestBody Map<String, String> body, HttpServletRequest request) {
        String username = body.get("username");
        String password = body.get("password");
        if (username == null || password == null) {
            return JsonResult.error("用户名和密码不能为空");
        }
        try {
            return JsonResult.success(authService.loginWithToken(username, password, request), "登录成功");
        } catch (AuthenticationException e) {
            return JsonResult.error(e.getMessage());
        }
    }

    /**
     * Token 登录（API/移动端场景）：校验密码后签发 Bearer 令牌。
     * <p>请求体：{"username":"...","password":"..."}
     * <p>返回：{"accessToken":"...","tokenType":"Bearer","expiresIn":43200}
     */
    @PostMapping("/token")
    public JsonResult token(@RequestBody Map<String, String> body, HttpServletRequest request) {
        String username = body.get("username");
        String password = body.get("password");
        if (username == null || password == null) {
            return JsonResult.error("用户名和密码不能为空");
        }
        try {
            return JsonResult.success(authService.issueToken(username, password, request), "令牌签发成功");
        } catch (AuthenticationException e) {
            return JsonResult.error(e.getMessage());
        }
    }

    /**
     * 登出：吊销 Token（若有）并清除会话。
     */
    @PostMapping("/logout")
    public JsonResult logout(HttpServletRequest request) {
        String bearer = BearerToken.extract(request);
        authService.logout(bearer);
        return JsonResult.success("已登出");
    }

    /**
     * 当前登录用户信息（角色 + 权限），返回 vben-admin 期望的 UserInfo 格式。
     */
    @GetMapping("/info")
    public JsonResult info() {
        Map<String, Object> info = authService.currentUser();
        if (info == null) {
            return JsonResult.error("未登录");
        }
        return JsonResult.success(info);
    }

    /**
     * 当前登录用户的权限码列表（供前端 getAccessCodesApi 使用）。
     * <p>返回 Shiro 权限标识数组，如 ["sys:user:add", "sys:role:*"]。
     */
    @GetMapping("/codes")
    public JsonResult codes() {
        return JsonResult.success(authService.getAccessCodes());
    }

    /**
     * 保存当前登录用户的前端偏好设置。
     * <p>请求体：{"preferences": "{...JSON字符串...}"}
     * <p>前端 preferencesManager 在用户修改偏好后通过 debounce 调用本接口，
     * 将自定义偏好持久化到 sys_user.preferences 列。
     */
    @PutMapping("/preferences")
    public JsonResult savePreferences(@RequestBody Map<String, String> body) {
        String preferences = body.get("preferences");
        if (preferences == null) {
            return JsonResult.error("preferences 不能为空");
        }
        try {
            authService.savePreferences(preferences);
            return JsonResult.success(null, "保存成功");
        } catch (Exception e) {
            return JsonResult.error(e.getMessage());
        }
    }
}
