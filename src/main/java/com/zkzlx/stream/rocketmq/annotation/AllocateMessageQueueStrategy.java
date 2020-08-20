package com.zkzlx.stream.rocketmq.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * mark Queue allocation algorithm specifying how message queues are allocated to each consumer clients.
 *  use annotation must implement {@link org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy}
 *
 *
 *
 *
 * @author junboXiang
 *
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface AllocateMessageQueueStrategy {

    String value() default "";

}
