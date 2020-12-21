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

package com.zkzlx.stream.rocketmq.properties;

import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

/**
 * Extended consumer properties for RocketMQ binder.
 *
 * @author zkzlx
 */
public class RocketMQConsumerProperties extends RocketMQCommonProperties {

	/**
	 * Message model defines the way how messages are delivered to each consumer clients.
	 * </p>
	 *
	 * This field defaults to clustering.
	 */
	private String messageModel = MessageModel.CLUSTERING.getModeCN();


	/**
	 * Queue allocation algorithm specifying how message queues are allocated to each
	 * consumer clients.
	 */
	private String allocateMessageQueueStrategy;

	/**
	 * for concurrently listener. message consume retry strategy. see
	 * {@link ConsumeConcurrentlyContext#getDelayLevelWhenNextConsume()}. -1 means dlq(or
	 * discard, see {@link this#shouldRequeue}), others means requeue.
	 */
	private int delayLevelWhenNextConsume = 0;

	/**
	 * see{@link ConsumeOrderlyContext#getSuspendCurrentQueueTimeMillis()}
	 */
	private int suspendCurrentQueueTimeMillis=1000;

	/**
	 * The expressions include tags or SQL,as follow:
	 * <p/>
	 * tag: {@code tag1||tag2||tag3 }; sql: {@code 'color'='blue' AND 'price'>100 } .
	 * <p/>
	 * Determines whether there are specific characters "{@code ||}" in the expression to
	 * determine how the message is filtered,tags or SQL.
	 */
	private String subscription ;


	private Push push;

	public String getMessageModel() {
		return messageModel;
	}

	public RocketMQConsumerProperties setMessageModel(String messageModel) {
		this.messageModel = messageModel;
		return this;
	}


	public String getAllocateMessageQueueStrategy() {
		return allocateMessageQueueStrategy;
	}

	public void setAllocateMessageQueueStrategy(String allocateMessageQueueStrategy) {
		this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
	}

	public String getSubscription() {
		return subscription;
	}

	public void setSubscription(String subscription) {
		this.subscription = subscription;
	}

	public int getDelayLevelWhenNextConsume() {
		return delayLevelWhenNextConsume;
	}

	public RocketMQConsumerProperties setDelayLevelWhenNextConsume(int delayLevelWhenNextConsume) {
		this.delayLevelWhenNextConsume = delayLevelWhenNextConsume;
		return this;
	}

	public int getSuspendCurrentQueueTimeMillis() {
		return suspendCurrentQueueTimeMillis;
	}

	public RocketMQConsumerProperties setSuspendCurrentQueueTimeMillis(int suspendCurrentQueueTimeMillis) {
		this.suspendCurrentQueueTimeMillis = suspendCurrentQueueTimeMillis;
		return this;
	}

	public Push getPush() {
		return push;
	}

	public void setPush(Push push) {
		this.push = push;
	}


	public boolean shouldRequeue() {
		return delayLevelWhenNextConsume != -1;
	}

	public static class Push{

		/**
		 * if orderly is true, using {@link MessageListenerOrderly} else if orderly if false,
		 * using {@link MessageListenerConcurrently}.
		 */
		private boolean orderly=false;

		public boolean getOrderly() {
			return orderly;
		}

		public Push setOrderly(boolean orderly) {
			this.orderly = orderly;
			return this;
		}
	}


}
