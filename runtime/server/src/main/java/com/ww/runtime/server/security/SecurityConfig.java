package com.ww.runtime.server.security;

import java.util.List;

import jakarta.servlet.http.HttpServletResponse;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;

import static org.springframework.security.web.servlet.util.matcher.PathPatternRequestMatcher.withDefaults;

/**
 * Spring Security 装配（Security 7，lambda DSL）：
 * <ul>
 *   <li>无状态会话：不创建/不使用 HttpSession，凭证为不透明 Bearer token；</li>
 *   <li>白名单：登录端点、OPTIONS 预检、静态资源（index.html/assets）；其余一律认证（default-deny）；</li>
 *   <li>CORS 统一由 {@link CorsConfigurationSource} 处理（原 MVC CORS 移除）；</li>
 *   <li>401/403 均返回 JSON 错误体；方法级权限经 {@link EnableMethodSecurity} 开启。</li>
 * </ul>
 */
@Configuration
@EnableWebSecurity
@EnableMethodSecurity
public class SecurityConfig {

    /** 认证入口：未认证访问受保护资源 → 401 JSON。 */
    private static final org.springframework.security.web.AuthenticationEntryPoint REST_ENTRY_POINT =
            (request, response, authException) ->
                    writeError(response, HttpServletResponse.SC_UNAUTHORIZED, "未登录或会话已过期，请重新登录");

    /** 拒绝访问：已认证但权限不足 → 403 JSON。 */
    private static final org.springframework.security.web.access.AccessDeniedHandler REST_ACCESS_DENIED =
            (request, response, accessDeniedException) ->
                    writeError(response, HttpServletResponse.SC_FORBIDDEN, "没有访问权限");

    @Bean
    SecurityFilterChain filterChain(HttpSecurity http,
                                    BearerTokenAuthenticationFilter bearerFilter) throws Exception {
        http
                .cors(Customizer.withDefaults())
                .csrf(AbstractHttpConfigurer::disable)
                .httpBasic(AbstractHttpConfigurer::disable)
                .formLogin(AbstractHttpConfigurer::disable)
                .logout(AbstractHttpConfigurer::disable)
                .sessionManagement(session ->
                        session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(authorize -> authorize
                        .requestMatchers(withDefaults().matcher(HttpMethod.OPTIONS, "/**")).permitAll()
                        // 内部 ASYNC 重入不重复鉴权（初始 REQUEST 已授权），避免异步响应已提交后 Access Denied
                        .dispatcherTypeMatchers(jakarta.servlet.DispatcherType.ASYNC).permitAll()
                        // 异步请求失败时容器向 /error 发起的 ERROR 分发同样无 SecurityContext
                        // （STATELESS 无会话 + 异步线程），不放行会重演「响应已提交后 Access Denied」级联
                        .dispatcherTypeMatchers(jakarta.servlet.DispatcherType.ERROR).permitAll()
                        .requestMatchers(withDefaults().matcher("/api/auth/login")).permitAll()
                        // 存活/就绪探针：部署脚本与编排系统的健康检查必须无凭证可用，
                        // 响应体仅含 UP/STARTING，不含任何集群细节
                        .requestMatchers(withDefaults().matcher("/rt/health")).permitAll()
                        // LB 压测回显探针：仅 GET、响应仅节点 id/端口/时间戳（无业务数据），
                        // 供 JMeter 等工具无凭证压测反代分发链路；仅限内网数据面使用
                        .requestMatchers(withDefaults().matcher(HttpMethod.GET, "/cache/lb-echo")).permitAll()
                        .requestMatchers(
                                withDefaults().matcher("/"),
                                withDefaults().matcher("/index.html"),
                                withDefaults().matcher("/favicon.ico"),
                                withDefaults().matcher("/assets/**")).permitAll()
                        .anyRequest().authenticated())
                .addFilterBefore(bearerFilter, UsernamePasswordAuthenticationFilter.class)
                .exceptionHandling(exceptions -> exceptions
                        .authenticationEntryPoint(REST_ENTRY_POINT)
                        .accessDeniedHandler(REST_ACCESS_DENIED));
        return http.build();
    }

    @Bean
    PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    /** CORS 配置源：管理 UI 跨节点直连数据面端点，内网管理面全放行（语义同原 CorsConfig）。 */
    @Bean
    CorsConfigurationSource corsConfigurationSource() {
        CorsConfiguration configuration = new CorsConfiguration();
        configuration.setAllowedOriginPatterns(List.of("*"));
        configuration.setAllowedMethods(List.of("GET", "PUT", "POST", "DELETE", "OPTIONS"));
        configuration.setAllowedHeaders(List.of("*"));
        configuration.setMaxAge(3600L);
        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/**", configuration);
        return source;
    }

    private static void writeError(HttpServletResponse response, int status, String message) throws java.io.IOException {
        response.setStatus(status);
        response.setContentType("application/json;charset=UTF-8");
        response.getWriter().write("{\"message\":\"" + message + "\"}");
    }
}
