package com.zkzlx.stream.rocketmq.custom;

import java.util.stream.Stream;

import com.zkzlx.stream.rocketmq.extend.ErrorAcknowledgeHandler;

import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.hook.CheckForbiddenHook;
import org.apache.rocketmq.client.hook.SendMessageHook;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.TransactionListener;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.messaging.converter.CompositeMessageConverter;

/**
 * find RocketMQ bean by annotations
 *
 * @author junboXiang
 *
 */
public class RocketMQConfigBeanPostProcessor implements BeanPostProcessor {

	@Override
	public Object postProcessAfterInitialization(Object bean, String beanName)
			throws BeansException {
		Stream.of(RocketMQBeanContainerCache.getClassAry()).forEach(clazz -> {
			if (clazz.isAssignableFrom(bean.getClass())) {
				RocketMQBeanContainerCache.putBean(beanName, bean);
			}
		});
		return bean;
	}

}
