/*
 * Copyright 2013-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zkzlx.stream.rocketmq.integration.inbound.poll;

import java.util.List;

import com.zkzlx.stream.rocketmq.integration.inbound.RocketMQConsumerFactory;
import com.zkzlx.stream.rocketmq.properties.RocketMQConsumerProperties;
import com.zkzlx.stream.rocketmq.support.RocketMQMessageConverterSupport;
import com.zkzlx.stream.rocketmq.utils.RocketMQUtils;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.Lifecycle;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.integration.endpoint.AbstractMessageSource;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;
import org.springframework.util.CollectionUtils;

/**
 * @author <a href="mailto:fangjian0423@gmail.com">Jim</a>
 */
public class RocketMQMessageSource extends AbstractMessageSource<Object>
		implements DisposableBean, Lifecycle {

	private final static Logger log = LoggerFactory
			.getLogger(RocketMQMessageSource.class);



	private DefaultLitePullConsumer consumer;

	private volatile boolean running;


	private final String topic;
	private final String group;
	private final MessageSelector messageSelector;
	private final RocketMQConsumerProperties consumerProperties;

	public RocketMQMessageSource(String name, String group, RocketMQConsumerProperties consumerProperties) {
		this.topic = name;
		this.group = group;
		this.messageSelector = RocketMQUtils.getMessageSelector(
				consumerProperties.getSubscription());
		this.consumerProperties = consumerProperties;

	}

	@Override
	public synchronized void start() {
		if (this.isRunning()) {
			throw new IllegalStateException(
					"pull consumer already running. " + this.toString());
		}
		try {
			this.consumer = RocketMQConsumerFactory.initPullConsumer(consumerProperties);
			//This parameter must be 1, otherwise doReceive cannot be handled singly.
			this.consumer.setPullBatchSize(1);
			this.consumer.subscribe(topic, messageSelector);
//			this.consumer.setAutoCommit(false);
			this.consumer.start();
		}
		catch (MQClientException e) {
			log.error("DefaultMQPullConsumer startup error: " + e.getMessage(), e);
		}
		this.running = true;
	}

	@Override
	public synchronized void stop() {
		if (this.isRunning() && null != consumer) {
			consumer.unsubscribe(topic);
			consumer.shutdown();
			this.running = false;
		}
	}

	@Override
	public synchronized boolean isRunning() {
		return running;
	}

	@Override
	protected synchronized Object doReceive() {
		List<MessageExt> messageExtList = consumer.poll();
		if(CollectionUtils.isEmpty(messageExtList) || messageExtList.size()>1){
			log.warn("========================"+ String.valueOf(messageExtList));
			return null;
//			throw new MessagingException("The result of this message pull is not right and is not being processed.");
		}
		log.info("====================="+ messageExtList.get(0));
		Message message = RocketMQMessageConverterSupport.instance()
				.convertMessage2Spring(messageExtList.get(0));
		//这里要处理消费结果，如果失败则。。。。。
		return MessageBuilder.fromMessage(message).setHeader(
				IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK,
				new RocketMQAckCallback(messageExtList.get(0))
				).build();
	}

	@Override
	public String getComponentType() {
		return "rocketmq:message-source";
	}

}
