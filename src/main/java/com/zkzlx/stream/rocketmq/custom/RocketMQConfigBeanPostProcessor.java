package com.zkzlx.stream.rocketmq.custom;

import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.messaging.converter.CompositeMessageConverter;

import com.zkzlx.stream.rocketmq.extend.ErrorAcknowledgeHandler;

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
		if (bean instanceof AllocateMessageQueueStrategy) {
			RocketMQBeanContainerCache.putBean(beanName, bean);
		}
		else if (bean instanceof MessageListener) {
			RocketMQBeanContainerCache.putBean(beanName, bean);
		}
		else if (bean instanceof MessageQueueSelector) {
			RocketMQBeanContainerCache.putBean(beanName, bean);
		}
		else if (bean instanceof TransactionListener) {
			RocketMQBeanContainerCache.putBean(beanName, bean);
		}
		else if (bean instanceof SendCallback) {
			RocketMQBeanContainerCache.putBean(beanName, bean);
		}
		else if (bean instanceof ErrorAcknowledgeHandler) {
			RocketMQBeanContainerCache.putBean(beanName, bean);
		}
		else if (bean instanceof CompositeMessageConverter) {
			RocketMQBeanContainerCache.putBean(beanName, bean);
		}
		return bean;
	}

}
