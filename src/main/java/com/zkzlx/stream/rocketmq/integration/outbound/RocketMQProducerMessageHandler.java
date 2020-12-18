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

import java.util.List;

import com.zkzlx.stream.rocketmq.contants.RocketMQConst;
import com.zkzlx.stream.rocketmq.custom.RocketMQBeanContainerCache;
import com.zkzlx.stream.rocketmq.properties.RocketMQProducerProperties;
import com.zkzlx.stream.rocketmq.properties.RocketMQProducerProperties.SendType;
import com.zkzlx.stream.rocketmq.provisioning.selector.PartitionMessageQueueSelector;
import com.zkzlx.stream.rocketmq.support.RocketMQMessageConverterSupport;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
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
public class RocketMQProducerMessageHandler extends AbstractMessageHandler
		implements Lifecycle {

	private final static Logger log = LoggerFactory
			.getLogger(RocketMQProducerMessageHandler.class);

	private final RocketMQMessageConverterSupport messageConverterSupport = RocketMQMessageConverterSupport
			.instance();

	private volatile boolean running = false;

	private ErrorMessageStrategy errorMessageStrategy = new DefaultErrorMessageStrategy();
	private MessageChannel sendFailureChannel;
	private MessageConverterConfigurer.PartitioningInterceptor partitioningInterceptor;

	private final ProducerDestination destination;
	private final ExtendedProducerProperties<RocketMQProducerProperties> extendedProducerProperties;
	private final DefaultMQProducer defaultMQProducer;
	private final RocketMQProducerProperties mqProducerProperties;

	public RocketMQProducerMessageHandler(ProducerDestination destination,
			ExtendedProducerProperties<RocketMQProducerProperties> extendedProducerProperties,
			RocketMQProducerProperties mqProducerProperties) {
		this.destination = destination;
		this.extendedProducerProperties = extendedProducerProperties;
		this.defaultMQProducer = RocketMQProduceProcessor
				.initRocketMQProducer(destination.getName(), mqProducerProperties);
		this.mqProducerProperties = mqProducerProperties;
	}

	@Override
	public void start() {
		try {
			if (extendedProducerProperties.isPartitioned()) {
				try {
					List<MessageQueue> messageQueues = defaultMQProducer
							.fetchPublishMessageQueues(destination.getName());
					if (extendedProducerProperties.getPartitionCount() != messageQueues
							.size()) {
						logger.info(String.format(
								"The partition count of topic '%s' will change from '%s' to '%s'",
								destination.getName(),
								extendedProducerProperties.getPartitionCount(),
								messageQueues.size()));
						extendedProducerProperties
								.setPartitionCount(messageQueues.size());
						partitioningInterceptor.setPartitionCount(
								extendedProducerProperties.getPartitionCount());
					}
				}
				catch (MQClientException e) {
					logger.error("fetch publish message queues fail", e);
				}
			}
			defaultMQProducer.start();
			running = true;
		}
		catch (MQClientException e) {
			log.error("The defaultMQProducer startup failure !!!", e);
		}
	}

	@Override
	public void stop() {
		if (running) {
			defaultMQProducer.shutdown();
		}
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
				TransactionListener transactionListener = RocketMQBeanContainerCache
						.getBean(mqProducerProperties.getTransactionListener(),
								TransactionListener.class);
				if (transactionListener == null) {
					throw new MessagingException(
							"TransactionMQProducer must have a TransactionMQProducer !!! ");
				}
				((TransactionMQProducer) defaultMQProducer)
						.setTransactionListener(transactionListener);
				sendResult = defaultMQProducer.sendMessageInTransaction(
						messageConverterSupport.convertMessage2MQ(destination.getName(),
								message),
						message.getHeaders().get(RocketMQConst.USER_TRANSACTIONAL_ARGS));
			}
			else {
				MessageQueueSelector messageQueueSelector = RocketMQBeanContainerCache
						.getBean(mqProducerProperties.getMessageQueueSelector(),
								MessageQueueSelector.class);
				sendResult = this.send(message, messageQueueSelector,
						message.getHeaders());
			}
			if (sendResult != null
					&& !SendStatus.SEND_OK.equals(sendResult.getSendStatus())) {
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
				.convertMessage2MQ(destination.getName(), message);
		SendResult sendResult = new SendResult();
		sendResult.setSendStatus(SendStatus.SEND_OK);
		if (SendType.OneWay
				.equalsName(extendedProducerProperties.getExtension().getSendType())) {
			if (null != selector) {
				defaultMQProducer.sendOneway(mqMessage, selector, args);
			}
			else {
				defaultMQProducer.sendOneway(mqMessage);
			}
			return sendResult;
		}
		if (SendType.Sync
				.equalsName(extendedProducerProperties.getExtension().getSendType())) {
			if (null != selector) {
				return defaultMQProducer.send(mqMessage, selector, args);
			}
			return defaultMQProducer.send(mqMessage);
		}
		if (SendType.Async
				.equalsName(extendedProducerProperties.getExtension().getSendType())) {
			if (null != selector) {
				defaultMQProducer.send(mqMessage, selector, args,
						this.getSendCallback(message));
			}
			else {
				defaultMQProducer.send(mqMessage, this.getSendCallback(message));
			}
			return sendResult;
		}
		throw new MessagingException(
				"message hasn't been sent,cause by : the SendType must be in this values[OneWay, Async, Sync]");
	}

	private SendCallback getSendCallback(Message<?> message) {
		SendCallback sendCallback = this.getApplicationContext().getBean(
				extendedProducerProperties.getExtension().getSendCallBack(),
				SendCallback.class);
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
