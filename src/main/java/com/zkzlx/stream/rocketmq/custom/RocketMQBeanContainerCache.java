package com.zkzlx.stream.rocketmq.custom;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.hook.CheckForbiddenHook;
import org.apache.rocketmq.client.hook.SendMessageHook;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.util.StringUtils;

import com.zkzlx.stream.rocketmq.extend.ErrorAcknowledgeHandler;

/**
 * Gets the beans configured in the configuration file
 *
 * @author junboXiang
 */
public final class RocketMQBeanContainerCache {

	private static final Class<?>[] CLASSES = new Class[] {
			CompositeMessageConverter.class, AllocateMessageQueueStrategy.class,
			MessageQueueSelector.class, MessageListener.class, TransactionListener.class,
			SendCallback.class, CheckForbiddenHook.class, SendMessageHook.class,
			ErrorAcknowledgeHandler.class };

	private static final Map<String, Object> BEANS_CACHE = new ConcurrentHashMap<>();

	static void putBean(String beanName, Object beanObj) {
		BEANS_CACHE.put(beanName, beanObj);
	}

	static Class<?>[] getClassAry() {
		return CLASSES;
	}

	public static <T> T getBean(String beanName, Class<T> clazz) {
		return getBean(beanName, clazz, null);
	}

	public static <T> T getBean(String beanName, Class<T> clazz, T defaultObj) {
		if (StringUtils.isEmpty(beanName)) {
			return defaultObj;
		}
		Object obj = BEANS_CACHE.get(beanName);
		if (null == obj) {
			return defaultObj;
		}
		if (clazz.isAssignableFrom(obj.getClass())) {
			return (T) obj;
		}
		return defaultObj;
	}

}
