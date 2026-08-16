package com.ww.admin.security;

import com.ww.admin.entity.AccessToken;
import com.ww.admin.entity.SysMenu;
import com.ww.admin.entity.SysRole;
import com.ww.admin.entity.SysUser;
import com.ww.admin.service.AccessTokenService;
import com.ww.admin.service.UserService;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.IncorrectCredentialsException;
import org.apache.shiro.authc.LockedAccountException;
import org.apache.shiro.authc.SimpleAuthenticationInfo;
import org.apache.shiro.authc.UnknownAccountException;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.authc.credential.CredentialsMatcher;
import org.apache.shiro.authc.credential.PasswordService;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.SimpleAuthorizationInfo;
import org.apache.shiro.realm.AuthorizingRealm;
import org.apache.shiro.subject.PrincipalCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashSet;
import java.util.Set;

/**
 * 数据库 Realm：Shiro 认证与授权的核心。
 * <p>
 * 同时支持两种认证令牌：
 * <ul>
 *   <li>{@link UsernamePasswordToken} —— Session 认证（用户名+密码登录）。</li>
 *   <li>{@link BearerToken} —— Token 认证（Bearer 令牌）。</li>
 * </ul>
 * 授权信息（角色 + 权限）全部从数据库加载：
 * <ul>
 *   <li>角色取 {@link SysRole#getRoleKey()}（如 admin, manager）。</li>
 *   <li>权限取 {@link SysMenu#getPerms()}（Shiro 通配符格式，如 sys:user:create / *:*）。</li>
 *   <li>用户状态由 {@link SysUser#getStatus()} 控制（0禁用 1正常）。</li>
 * </ul>
 * 权限映射持久化在数据库（sys_menu + sys_role_menu + sys_user_role），而非 ini 配置文件。
 */
public class DbRealm extends AuthorizingRealm {

    private static final Logger log = LoggerFactory.getLogger(DbRealm.class);

    private final UserService userService;
    private final AccessTokenService accessTokenService;
    private final PasswordService passwordService;

    @Autowired
    public DbRealm(UserService userService,
                   AccessTokenService accessTokenService,
                   PasswordService passwordService) {
        this.userService = userService;
        this.accessTokenService = accessTokenService;
        this.passwordService = passwordService;
        // 自定义凭证匹配器：BearerToken 已在 doGetAuthenticationInfo 中通过 DB 校验；
        // UsernamePasswordToken 委托 PasswordService 校验哈希。
        setCredentialsMatcher((CredentialsMatcher) (token, info) -> {
            if (token instanceof BearerToken) {
                return true;
            }
            Object stored = info.getCredentials();
            return passwordService.passwordsMatch(token.getCredentials(), stored == null ? null : stored.toString());
        });
    }

    @Override
    public boolean supports(AuthenticationToken token) {
        return token instanceof UsernamePasswordToken || token instanceof BearerToken;
    }

    @Override
    protected AuthenticationInfo doGetAuthenticationInfo(AuthenticationToken token) throws AuthenticationException {
        if (token instanceof BearerToken bearer) {
            return authenticateByToken(bearer);
        }
        return authenticateByUsernamePassword((UsernamePasswordToken) token);
    }

    /**
     * Token 认证：按令牌哈希查库，校验未过期/未吊销。
     */
    private AuthenticationInfo authenticateByToken(BearerToken bearer) {
        AccessToken accessToken = accessTokenService.findValid(bearer.getToken());
        if (accessToken == null) {
            throw new IncorrectCredentialsException("令牌无效或已过期");
        }
        SysUser user = accessToken.getUser();
        if (user == null || (user.getStatus() != null && user.getStatus() != 1)) {
            throw new LockedAccountException("账号不可用");
        }
        // principal 用用户名，与用户名密码登录保持一致，便于授权阶段复用
        return new SimpleAuthenticationInfo(user.getUsername(), bearer.getToken(), getName());
    }

    /**
     * 用户名密码认证。
     */
    private AuthenticationInfo authenticateByUsernamePassword(UsernamePasswordToken token) {
        String username = (String) token.getPrincipal();
        SysUser user = userService.findByUsername(username);
        if (user == null) {
            throw new UnknownAccountException("账号不存在");
        }
        if (user.getStatus() != null && user.getStatus() != 1) {
            throw new LockedAccountException("账号已禁用");
        }
        return new SimpleAuthenticationInfo(user.getUsername(), user.getPassword(), getName());
    }

    @Override
    protected AuthorizationInfo doGetAuthorizationInfo(PrincipalCollection principals) {
        String username = (String) principals.getPrimaryPrincipal();
        SysUser user = userService.findByUsername(username);
        if (user == null) {
            log.warn("授权阶段用户不存在: {}", username);
            return null;
        }
        SimpleAuthorizationInfo info = new SimpleAuthorizationInfo();
        Set<String> roleKeys = new HashSet<>();
        Set<String> permissionCodes = new HashSet<>();
        if (user.getRoles() != null) {
            for (SysRole role : user.getRoles()) {
                if (role.getStatus() != null && role.getStatus() != 1) {
                    continue;
                }
                roleKeys.add(role.getRoleKey());
                if (role.getMenus() != null) {
                    for (SysMenu menu : role.getMenus()) {
                        if (menu.getStatus() != null && menu.getStatus() != 1) {
                            continue;
                        }
                        if (menu.getPerms() != null && !menu.getPerms().isEmpty()) {
                            permissionCodes.add(menu.getPerms());
                        }
                    }
                }
            }
        }
        info.setRoles(roleKeys);
        info.setStringPermissions(permissionCodes);
        return info;
    }
}
