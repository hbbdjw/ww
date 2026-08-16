package com.ww.admin.config;

import com.blazebit.persistence.CriteriaBuilderFactory;
import com.blazebit.persistence.spi.CriteriaBuilderConfiguration;
import com.blazebit.persistence.spi.CriteriaBuilderConfigurationProvider;
import com.blazebit.persistence.view.EntityViewManager;
import com.blazebit.persistence.view.impl.EntityViewConfigurationImpl;
import com.blazebit.persistence.view.spi.EntityViewConfiguration;
import com.ww.admin.entity.SysUserView;
import jakarta.persistence.EntityManagerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ServiceLoader;

/**
 * Blaze-Persistence 配置 — CriteriaBuilderFactory + EntityViewManager
 * 与 Hibernate 7（jakarta）集成，供 BaseDaoImpl 暴露给 Service 层使用。
 */
@Configuration
public class BlazePersistenceConfig {

    @Bean
    public CriteriaBuilderFactory criteriaBuilderFactory(EntityManagerFactory entityManagerFactory) {
        // ServiceLoader 加载 core-impl-jakarta 提供的 CriteriaBuilderConfigurationProvider
        CriteriaBuilderConfiguration config = ServiceLoader
                .load(CriteriaBuilderConfigurationProvider.class)
                .iterator()
                .next()
                .createConfiguration();
        return config.createCriteriaBuilderFactory(entityManagerFactory);
    }

    @Bean
    public EntityViewManager entityViewManager(CriteriaBuilderFactory cbf) {
        EntityViewConfiguration evc = new EntityViewConfigurationImpl();
        evc.addEntityView(SysUserView.class);   // 注册示例视图；后续新增视图在此追加
        return evc.createEntityViewManager(cbf);
    }
}
