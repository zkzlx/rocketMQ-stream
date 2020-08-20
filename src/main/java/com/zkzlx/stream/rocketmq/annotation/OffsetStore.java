package com.zkzlx.stream.rocketmq.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 *
 * mark producer OffsetStore
 * use annotation must implement {@link org.apache.rocketmq.client.consumer.store.OffsetStore}
 *
 *
 * @author junboXiang
 *
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface OffsetStore {

    String value() default "";

}
