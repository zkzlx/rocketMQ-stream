package com.zkzlx.stream.rocketmq.custom;

import java.util.stream.Stream;

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
		Stream.of(RocketMQBeanContainerCache.getClassAry()).forEach(clazz -> {
			if (clazz.isAssignableFrom(bean.getClass())) {
				RocketMQBeanContainerCache.putBean(beanName, bean);
			}
		});
		return bean;
	}

}
