/*
 * Copyright (C) 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zkzlx.stream.rocketmq.integration.outbound;

import com.zkzlx.stream.rocketmq.contants.RocketMQConst;
import com.zkzlx.stream.rocketmq.properties.RocketMQBinderConfigurationProperties;
import com.zkzlx.stream.rocketmq.properties.RocketMQProducerProperties;
import com.zkzlx.stream.rocketmq.properties.RocketMQProducerProperties.SendType;
import com.zkzlx.stream.rocketmq.support.RocketMQMessageConverterSupport;
import com.zkzlx.stream.rocketmq.support.RocketMQProducerConsumerSupport;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.selector.SelectMessageQueueByHash;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binding.MessageConverterConfigurer;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.context.Lifecycle;
import org.springframework.integration.handler.AbstractMessageHandler;
import org.springframework.integration.support.DefaultErrorMessageStrategy;
import org.springframework.integration.support.ErrorMessageStrategy;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessagingException;

/**
 * @author zkzlx
 */
public class RocketMQProducerMessageHandler extends AbstractMessageHandler implements  Lifecycle {

	private final static Logger log = LoggerFactory
			.getLogger(RocketMQProducerMessageHandler.class);

	private final RocketMQMessageConverterSupport messageConverterSupport = RocketMQMessageConverterSupport
			.instance();

	private volatile boolean running = false;

	private ErrorMessageStrategy errorMessageStrategy = new DefaultErrorMessageStrategy();
	private MessageChannel sendFailureChannel;
	private MessageConverterConfigurer.PartitioningInterceptor partitioningInterceptor;


	private final RocketMQBinderConfigurationProperties binderConfigurationProperties;
	private final ProducerDestination producerDestination;
	private final ExtendedProducerProperties<RocketMQProducerProperties> extendedProducerProperties;
	private final DefaultMQProducer defaultMQProducer;

	public RocketMQProducerMessageHandler(
			RocketMQBinderConfigurationProperties binderConfigurationProperties,
			ExtendedProducerProperties<RocketMQProducerProperties> extendedProducerProperties,
			ProducerDestination producerDestination) {
		this.binderConfigurationProperties = binderConfigurationProperties;
		this.producerDestination = producerDestination;
		this.extendedProducerProperties = extendedProducerProperties;
		this.defaultMQProducer = RocketMQProducerConsumerSupport.initRocketMQProducer(
				producerDestination, binderConfigurationProperties, extendedProducerProperties);
	}

	@Override
	public void start() {
		try {
			defaultMQProducer.start();
		}
		catch (MQClientException e) {
		}
		running = true;
	}

	@Override
	public void stop() {
		defaultMQProducer.shutdown();
		running = false;
	}

	@Override
	public boolean isRunning() {
		return running;
	}

	@Override
	protected void handleMessageInternal(Message<?> message) {
		try {
			SendResult sendResult;
			if (defaultMQProducer instanceof TransactionMQProducer) {
				sendResult = defaultMQProducer.sendMessageInTransaction(
						messageConverterSupport.convertMessage2MQ(producerDestination.getName(),
								message),
						message.getHeaders()
								.get(RocketMQConst.USER_TRANSACTIONAL_ARGS));
			}
			else {
				Object selectorArg = null;
				try {
					selectorArg = message.getHeaders()
							.getOrDefault(RocketMQConst.PROPERTY_DELAY_TIME_LEVEL, 0);
				}
				catch (Exception ignored) {
				}
				MessageQueueSelector messageQueueSelector = null;
				// is orderly ?
				if (null != selectorArg) {
					messageQueueSelector = this.getApplicationContext().getBean(
							extendedProducerProperties.getExtension().getMessageQueueSelector(),
							MessageQueueSelector.class);
					if (null == messageQueueSelector) {
						messageQueueSelector = new SelectMessageQueueByHash();
					}
				}
				sendResult = this.send(message, messageQueueSelector, selectorArg);
			}
			if (sendResult != null
					&& !sendResult.getSendStatus().equals(SendStatus.SEND_OK)) {
				if (getSendFailureChannel() != null) {
					this.getSendFailureChannel().send(message);
				}
				else {
					throw new MessagingException(message,
							new MQClientException("message hasn't been sent", null));
				}
			}
		}
		catch (Exception e) {
			log.error("RocketMQ Message hasn't been sent. Caused by " + e.getMessage());
			if (getSendFailureChannel() != null) {
				getSendFailureChannel().send(getErrorMessageStrategy()
						.buildErrorMessage(new MessagingException(message, e), null));
			}
			else {
				throw new MessagingException(message, e);
			}
		}

	}

	private SendResult send(Message<?> message, MessageQueueSelector selector,
			Object args) throws RemotingException, MQClientException,
			InterruptedException, MQBrokerException {
		org.apache.rocketmq.common.message.Message mqMessage = messageConverterSupport
				.convertMessage2MQ(producerDestination.getName(), message);
		if (SendType.OneWay.equalsName(extendedProducerProperties.getExtension().getSendType())) {
			if (null != selector) {
				defaultMQProducer.sendOneway(mqMessage, selector, args);
			}
			defaultMQProducer.sendOneway(mqMessage);
			return null;
		}
		if (SendType.Sync.equalsName(extendedProducerProperties.getExtension().getSendType())) {
			if (null != selector) {
				return defaultMQProducer.send(mqMessage, selector, args);
			}
			return defaultMQProducer.send(mqMessage);
		}
		if (SendType.Async.equalsName(extendedProducerProperties.getExtension().getSendType())) {
			if (null != selector) {
				defaultMQProducer.send(mqMessage, selector, args,
						this.getSendCallback(message));
			}
			defaultMQProducer.send(mqMessage, this.getSendCallback(message));
		}
		return null;
	}

	private SendCallback getSendCallback(Message<?> message) {
		SendCallback sendCallback = this.getApplicationContext().getBean(
				extendedProducerProperties.getExtension().getSendCallBack(), SendCallback.class);
		if (null == sendCallback) {
			sendCallback = new SendCallback() {
				@Override
				public void onSuccess(SendResult sendResult) {
				}

				@Override
				public void onException(Throwable e) {
					log.error("RocketMQ Message hasn't been sent. Caused by "
							+ e.getMessage());
					if (getSendFailureChannel() != null) {
						getSendFailureChannel()
								.send(getErrorMessageStrategy().buildErrorMessage(
										new MessagingException(message, e), null));
					}
				}
			};
		}
		return sendCallback;
	}

	public MessageChannel getSendFailureChannel() {
		return sendFailureChannel;
	}

	public void setSendFailureChannel(MessageChannel sendFailureChannel) {
		this.sendFailureChannel = sendFailureChannel;
	}

	public ErrorMessageStrategy getErrorMessageStrategy() {
		return errorMessageStrategy;
	}

	public void setErrorMessageStrategy(ErrorMessageStrategy errorMessageStrategy) {
		this.errorMessageStrategy = errorMessageStrategy;
	}
}
