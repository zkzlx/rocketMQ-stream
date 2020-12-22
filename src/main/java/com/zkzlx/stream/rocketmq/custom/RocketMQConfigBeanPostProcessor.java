package com.zkzlx.stream.rocketmq.custom;

import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;

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

		return bean;
	}

}
