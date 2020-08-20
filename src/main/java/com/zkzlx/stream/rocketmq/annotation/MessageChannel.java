package com.zkzlx.stream.rocketmq.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * mark producer config bean SendFailUreChannel
 * use annotation must implement {@link org.springframework.messaging.MessageChannel}
 * @author junboXiang
 *
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface MessageChannel {

    String value() default "";

}
