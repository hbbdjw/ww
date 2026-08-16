package com.ww.common.id;

import org.hibernate.annotations.IdGeneratorType;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Snowflake ID 主键注解
 */
@IdGeneratorType(SnowflakeIdGenerator.class)
@Target({METHOD, FIELD})
@Retention(RUNTIME)
public @interface SnowflakeId {
}
