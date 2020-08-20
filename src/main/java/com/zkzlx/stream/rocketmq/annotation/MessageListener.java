package com.zkzlx.stream.rocketmq.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * mark producer Message Listener
 * use annotation must implement {@link org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently}
 *
 *
 * @author junboXiang
 *
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface MessageListener {

    String value() default "";

}