package com.ww.runtime.server.lb;

import java.time.Duration;
import java.util.EnumSet;

import jakarta.servlet.DispatcherType;
import javax.sql.DataSource;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.ww.runtime.cluster.NodeRegistry;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.lb.HttpReverseProxy;
import com.ww.runtime.lb.InMemoryLbConfigStore;
import com.ww.runtime.lb.JdbcLbConfigStore;
import com.ww.runtime.lb.LbConfig;
import com.ww.runtime.lb.LbConfigStore;
import com.ww.runtime.lb.LbLoadBalancer;
import com.ww.runtime.server.RuntimeProperties;

/**
 * runtime-lb 的 Spring 装配（核心模块本身 Spring-free，全部接线在本类完成）。
 *
 * <p>过滤器注册顺序 -90：位于 springSecurityFilterChain(-100) 之后、DispatcherServlet 之前，
 * 保证入口先完成 Bearer 鉴权再分发；仅 REQUEST 分发（ASYNC 不重复代理）。</p>
 */
@Configuration
public class LbAssembly {

    /** springSecurityFilterChain 的 order 为 -100，LB 过滤器紧随其后。 */
    private static final int FILTER_ORDER = -90;

    @Bean
    LbConfigStore lbConfigStore(RuntimeProperties props, ObjectProvider<DataSource> dataSource) {
        if ("memory".equals(props.store())) {
            return new InMemoryLbConfigStore();
        }
        return new JdbcLbConfigStore(dataSource.getObject());
    }

    @Bean
    NodeRegistryLbDirectory lbNodeDirectory(NodeRegistry registry) {
        return new NodeRegistryLbDirectory(registry);
    }

    @Bean
    LbLoadBalancer lbLoadBalancer(NodeRegistryLbDirectory directory) {
        return new LbLoadBalancer(directory);
    }

    /** 连接超时取默认配置（HttpClient 连接池单例，进程期内不随配置重建）；请求总超时每次调用生效。 */
    @Bean
    HttpReverseProxy httpReverseProxy() {
        LbConfig defaults = LbConfig.defaultConfig();
        return new HttpReverseProxy(Duration.ofMillis(defaults.connectTimeoutMs()));
    }

    @Bean
    LbProxyFilter lbProxyFilter(NodeId self, LbLoadBalancer loadBalancer, HttpReverseProxy httpReverseProxy) {
        return new LbProxyFilter(self.value(), loadBalancer, httpReverseProxy);
    }

    @Bean
    LbRefresher lbRefresher(LbConfigStore store,
                            NodeRegistryLbDirectory directory,
                            LbProxyFilter lbProxyFilter) {
        return new LbRefresher(store, directory, lbProxyFilter);
    }

    @Bean
    LbAdminController lbAdminController(LbConfigStore store, LbRefresher lbRefresher) {
        return new LbAdminController(store, lbRefresher);
    }

    @Bean
    FilterRegistrationBean<LbProxyFilter> lbProxyFilterRegistration(LbProxyFilter filter) {
        FilterRegistrationBean<LbProxyFilter> registration = new FilterRegistrationBean<>(filter);
        registration.setOrder(FILTER_ORDER);
        registration.setDispatcherTypes(EnumSet.of(DispatcherType.REQUEST));
        registration.addUrlPatterns("/*");
        registration.setName("rtLbProxyFilter");
        return registration;
    }
}
