package com.zkzlx.stream.rocketmq.custom;

import com.zkzlx.stream.rocketmq.annotation.AllocateMessageQueueStrategy;
import com.zkzlx.stream.rocketmq.annotation.MessageChannel;
import com.zkzlx.stream.rocketmq.annotation.MessageListener;
import com.zkzlx.stream.rocketmq.annotation.OffsetStore;
import com.zkzlx.stream.rocketmq.support.RocketMQContainerSupport;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.StringUtils;

/**
 * find RocketMQ bean by annotations
 * {@link com.zkzlx.stream.rocketmq.annotation.AllocateMessageQueueStrategy}
 * {@link com.zkzlx.stream.rocketmq.annotation.MessageListener}
 * {@link com.zkzlx.stream.rocketmq.annotation.OffsetStore}
 * {@link com.zkzlx.stream.rocketmq.annotation.MessageChannel}
 *
 * @author junboXiang
 *
 */
public class RocketMQConfigBeanPostProcessor implements BeanPostProcessor {


    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        if (bean instanceof org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy) {
            AllocateMessageQueueStrategy annotation = AnnotationUtils.findAnnotation(bean.getClass(), AllocateMessageQueueStrategy.class);
            beanName = annotation == null ? beanName : StringUtils.hasText(annotation.value()) ? annotation.value() : annotation.value();
            RocketMQContainerSupport.putAllocateMessageQueueStrategyCache(beanName, org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy.class.cast(bean));
        } else if (bean instanceof org.apache.rocketmq.client.consumer.listener.MessageListener) {
            MessageListener annotation = AnnotationUtils.findAnnotation(bean.getClass(), MessageListener.class);
            beanName = annotation == null ? beanName : StringUtils.hasText(annotation.value()) ? annotation.value() : annotation.value();
            RocketMQContainerSupport.putMessageListenerCache(beanName, org.apache.rocketmq.client.consumer.listener.MessageListener.class.cast(bean));
        } else if (bean instanceof org.apache.rocketmq.client.consumer.store.OffsetStore) {
            OffsetStore annotation = AnnotationUtils.findAnnotation(bean.getClass(), OffsetStore.class);
            beanName = annotation == null ? beanName : StringUtils.hasText(annotation.value()) ? annotation.value() : annotation.value();
            RocketMQContainerSupport.putOffsetStoreCache(beanName, org.apache.rocketmq.client.consumer.store.OffsetStore.class.cast(bean));
        } else if (bean instanceof org.springframework.messaging.MessageChannel) {
            MessageChannel annotation = AnnotationUtils.findAnnotation(bean.getClass(), MessageChannel.class);
            beanName = annotation == null ? beanName : StringUtils.hasText(annotation.value()) ? annotation.value() : annotation.value();
            RocketMQContainerSupport.putMessageChannelCache(beanName, org.springframework.messaging.MessageChannel.class.cast(bean));
        }
        return bean;
    }

}
