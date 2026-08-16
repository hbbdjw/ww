package com.ww.common.id;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.id.IdentifierGenerator;

/**
 * Hibernate 6/7 Snowflake ID 生成器
 */
public class SnowflakeIdGenerator implements IdentifierGenerator {

    private static final Snowflake SNOWFLAKE = new Snowflake(1, 1);

    @Override
    public Object generate(SharedSessionContractImplementor session, Object object) {
        return SNOWFLAKE.nextId();
    }
}
