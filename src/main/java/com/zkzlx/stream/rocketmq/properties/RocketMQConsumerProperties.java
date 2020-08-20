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

import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.consumer.MessageQueueListener;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.trace.TraceDispatcher;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
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
	 * RocketMQ supports two message models: clustering and broadcasting. If clustering is
	 * set, consumer clients with the same {@link #consumerGroup} would only consume
	 * shards of the messages subscribed, which achieves load balances; Conversely, if the
	 * broadcasting is set, each consumer client will consume all subscribed messages
	 * separately.
	 * </p>
	 *
	 * This field defaults to clustering.
	 */
	private MessageModel messageModel = MessageModel.CLUSTERING;

	/**
	 * Offset Storage
	 */
	private OffsetStore offsetStore;

	/**
	 * Queue allocation algorithm specifying how message queues are allocated to each
	 * consumer clients.
	 */
	private AllocateMessageQueueStrategy allocateMessageQueueStrategy;

	/**
	 * Maximum number of messages pulled each time.
	 */
	private int pullBatchSize = 32;
	/**
	 * Flow control threshold on queue level, each message queue will cache at most 1000
	 * messages by default, Consider the {@code pullBatchSize}, the instantaneous value
	 * may exceed the limit
	 */
	private int pullThresholdForQueue = 1000;

	/**
	 * Limit the cached message size on queue level, each message queue will cache at most
	 * 100 MiB messages by default, Consider the {@code pullBatchSize}, the instantaneous
	 * value may exceed the limit
	 *
	 * <p>
	 * The size of a message only measured by message body, so it's not accurate
	 */
	private int pullThresholdSizeForQueue = 100;
	/**
	 * Consuming point on consumer booting.
	 * </p>
	 *
	 * There are three consuming points:
	 * <ul>
	 * <li><code>CONSUME_FROM_LAST_OFFSET</code>: consumer clients pick up where it
	 * stopped previously. If it were a newly booting up consumer client, according aging
	 * of the consumer group, there are two cases:
	 * <ol>
	 * <li>if the consumer group is created so recently that the earliest message being
	 * subscribed has yet expired, which means the consumer group represents a lately
	 * launched business, consuming will start from the very beginning;</li>
	 * <li>if the earliest message being subscribed has expired, consuming will start from
	 * the latest messages, meaning messages born prior to the booting timestamp would be
	 * ignored.</li>
	 * </ol>
	 * </li>
	 * <li><code>CONSUME_FROM_FIRST_OFFSET</code>: Consumer client will start from
	 * earliest messages available.</li>
	 * <li><code>CONSUME_FROM_TIMESTAMP</code>: Consumer client will start from specified
	 * timestamp, which means messages born prior to {@link #consumeTimestamp} will be
	 * ignored</li>
	 * </ul>
	 */
	private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;
	/**
	 * Backtracking consumption time with second precision. Time format is
	 * 20131223171201<br>
	 * Implying Seventeen twelve and 01 seconds on December 23, 2013 year<br>
	 * Default backtracking consumption time Half an hour ago.
	 */
	private String consumeTimestamp = UtilAll
			.timeMillisToHumanString3(System.currentTimeMillis() - (1000 * 60 * 30));

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

	private Poll poll;


	public MessageModel getMessageModel() {
		return messageModel;
	}

	public void setMessageModel(MessageModel messageModel) {
		this.messageModel = messageModel;
	}

	public OffsetStore getOffsetStore() {
		return offsetStore;
	}

	public void setOffsetStore(OffsetStore offsetStore) {
		this.offsetStore = offsetStore;
	}

	public AllocateMessageQueueStrategy getAllocateMessageQueueStrategy() {
		return allocateMessageQueueStrategy;
	}

	public void setAllocateMessageQueueStrategy(AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
		this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
	}

	public int getPullBatchSize() {
		return pullBatchSize;
	}

	public void setPullBatchSize(int pullBatchSize) {
		this.pullBatchSize = pullBatchSize;
	}

	public int getPullThresholdForQueue() {
		return pullThresholdForQueue;
	}

	public void setPullThresholdForQueue(int pullThresholdForQueue) {
		this.pullThresholdForQueue = pullThresholdForQueue;
	}

	public int getPullThresholdSizeForQueue() {
		return pullThresholdSizeForQueue;
	}

	public void setPullThresholdSizeForQueue(int pullThresholdSizeForQueue) {
		this.pullThresholdSizeForQueue = pullThresholdSizeForQueue;
	}

	public ConsumeFromWhere getConsumeFromWhere() {
		return consumeFromWhere;
	}

	public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
		this.consumeFromWhere = consumeFromWhere;
	}

	public String getConsumeTimestamp() {
		return consumeTimestamp;
	}

	public void setConsumeTimestamp(String consumeTimestamp) {
		this.consumeTimestamp = consumeTimestamp;
	}

	public String getSubscription() {
		return subscription;
	}

	public void setSubscription(String subscription) {
		this.subscription = subscription;
	}

	public Push getPush() {
		return push;
	}

	public void setPush(Push push) {
		this.push = push;
	}

	public Poll getPoll() {
		return poll;
	}

	public void setPoll(Poll poll) {
		this.poll = poll;
	}





	public static class Push{

		/**
		 * Message listener
		 */
		private MessageListener messageListener;

		/**
		 * Threshold for dynamic adjustment of the number of thread pool
		 */
		private long adjustThreadPoolNumsThreshold = 100000;

		/**
		 * Concurrently max span offset.it has no effect on sequential consumption
		 */
		private int consumeConcurrentlyMaxSpan = 2000;

		/**
		 * Flow control threshold on topic level, default value is -1(Unlimited)
		 * <p>
		 * The value of {@code pullThresholdForQueue} will be overwrote and calculated based
		 * on {@code pullThresholdForTopic} if it is't unlimited
		 * <p>
		 * For example, if the value of pullThresholdForTopic is 1000 and 10 message queues
		 * are assigned to this consumer, then pullThresholdForQueue will be set to 100
		 */
		private int pullThresholdForTopic = -1;

		/**
		 * Limit the cached message size on topic level, default value is -1 MiB(Unlimited)
		 * <p>
		 * The value of {@code pullThresholdSizeForQueue} will be overwrote and calculated
		 * based on {@code pullThresholdSizeForTopic} if it is't unlimited
		 * <p>
		 * For example, if the value of pullThresholdSizeForTopic is 1000 MiB and 10 message
		 * queues are assigned to this consumer, then pullThresholdSizeForQueue will be set to
		 * 100 MiB
		 */
		private int pullThresholdSizeForTopic = -1;

		/**
		 * Message pull Interval
		 */
		private long pullInterval = 0;

		/**
		 * Batch consumption size
		 */
		private int consumeMessageBatchMaxSize = 1;

		/**
		 * Whether update subscription relationship when every pull
		 */
		private boolean postSubscriptionWhenPull = false;

		/**
		 * Max re-consume times. -1 means 16 times.
		 * </p>
		 *
		 * If messages are re-consumed more than {@link #maxReconsumeTimes} before success,
		 * it's be directed to a deletion queue waiting.
		 */
		private int maxReconsumeTimes = -1;

		/**
		 * Suspending pulling time for cases requiring slow pulling like flow-control
		 * scenario.
		 */
		private long suspendCurrentQueueTimeMillis = 1000;

		/**
		 * Maximum amount of time in minutes a message may block the consuming thread.
		 */
		private long consumeTimeout = 15;

		/**
		 * Maximum time to await message consuming when shutdown consumer, 0 indicates no
		 * await.
		 */
		private long awaitTerminationMillisWhenShutdown = 0;

		/**
		 * Interface of asynchronous transfer data
		 */
		private TraceDispatcher traceDispatcher = null;


		public MessageListener getMessageListener() {
			return messageListener;
		}

		public void setMessageListener(MessageListener messageListener) {
			this.messageListener = messageListener;
		}

		public long getAdjustThreadPoolNumsThreshold() {
			return adjustThreadPoolNumsThreshold;
		}

		public void setAdjustThreadPoolNumsThreshold(long adjustThreadPoolNumsThreshold) {
			this.adjustThreadPoolNumsThreshold = adjustThreadPoolNumsThreshold;
		}

		public int getConsumeConcurrentlyMaxSpan() {
			return consumeConcurrentlyMaxSpan;
		}

		public void setConsumeConcurrentlyMaxSpan(int consumeConcurrentlyMaxSpan) {
			this.consumeConcurrentlyMaxSpan = consumeConcurrentlyMaxSpan;
		}

		public int getPullThresholdForTopic() {
			return pullThresholdForTopic;
		}

		public void setPullThresholdForTopic(int pullThresholdForTopic) {
			this.pullThresholdForTopic = pullThresholdForTopic;
		}

		public int getPullThresholdSizeForTopic() {
			return pullThresholdSizeForTopic;
		}

		public void setPullThresholdSizeForTopic(int pullThresholdSizeForTopic) {
			this.pullThresholdSizeForTopic = pullThresholdSizeForTopic;
		}

		public long getPullInterval() {
			return pullInterval;
		}

		public void setPullInterval(long pullInterval) {
			this.pullInterval = pullInterval;
		}

		public int getConsumeMessageBatchMaxSize() {
			return consumeMessageBatchMaxSize;
		}

		public void setConsumeMessageBatchMaxSize(int consumeMessageBatchMaxSize) {
			this.consumeMessageBatchMaxSize = consumeMessageBatchMaxSize;
		}

		public boolean getPostSubscriptionWhenPull() {
			return postSubscriptionWhenPull;
		}

		public void setPostSubscriptionWhenPull(boolean postSubscriptionWhenPull) {
			this.postSubscriptionWhenPull = postSubscriptionWhenPull;
		}

		public int getMaxReconsumeTimes() {
			return maxReconsumeTimes;
		}

		public void setMaxReconsumeTimes(int maxReconsumeTimes) {
			this.maxReconsumeTimes = maxReconsumeTimes;
		}

		public long getSuspendCurrentQueueTimeMillis() {
			return suspendCurrentQueueTimeMillis;
		}

		public void setSuspendCurrentQueueTimeMillis(long suspendCurrentQueueTimeMillis) {
			this.suspendCurrentQueueTimeMillis = suspendCurrentQueueTimeMillis;
		}

		public long getConsumeTimeout() {
			return consumeTimeout;
		}

		public void setConsumeTimeout(long consumeTimeout) {
			this.consumeTimeout = consumeTimeout;
		}

		public long getAwaitTerminationMillisWhenShutdown() {
			return awaitTerminationMillisWhenShutdown;
		}

		public void setAwaitTerminationMillisWhenShutdown(long awaitTerminationMillisWhenShutdown) {
			this.awaitTerminationMillisWhenShutdown = awaitTerminationMillisWhenShutdown;
		}

		public TraceDispatcher getTraceDispatcher() {
			return traceDispatcher;
		}

		public void setTraceDispatcher(TraceDispatcher traceDispatcher) {
			this.traceDispatcher = traceDispatcher;
		}
	}


	public static class Poll{

		/**
		 * Long polling mode, the Consumer connection max suspend time, it is not recommended
		 * to modify
		 */
		private long brokerSuspendMaxTimeMillis = 1000 * 20;

		/**
		 * Long polling mode, the Consumer connection timeout(must greater than
		 * brokerSuspendMaxTimeMillis), it is not recommended to modify
		 */
		private long consumerTimeoutMillisWhenSuspend = 1000 * 30;

		/**
		 * The socket timeout in milliseconds
		 */
		private long consumerPullTimeoutMillis = 1000 * 10;

		/**
		 * Message queue listener
		 */
		private MessageQueueListener messageQueueListener;

		/**
		 * The flag for auto commit offset
		 */
		private boolean autoCommit = true;

		/**
		 * Maximum commit offset interval time in milliseconds.
		 */
		private long autoCommitIntervalMillis = 5 * 1000;

		/**
		 * Flow control threshold for consume request, each consumer will cache at most 10000
		 * consume requests by default. Consider the {@code pullBatchSize}, the instantaneous
		 * value may exceed the limit
		 */
		private long pullThresholdForAll = 10000;

		/**
		 * Consume max span offset.
		 */
		private int consumeMaxSpan = 2000;

		/**
		 * The poll timeout in milliseconds
		 */
		private long pollTimeoutMillis = 1000 * 5;

		/**
		 * Interval time in in milliseconds for checking changes in topic metadata.
		 */
		private long topicMetadataCheckIntervalMillis = 30 * 1000;


		public long getBrokerSuspendMaxTimeMillis() {
			return brokerSuspendMaxTimeMillis;
		}

		public void setBrokerSuspendMaxTimeMillis(long brokerSuspendMaxTimeMillis) {
			this.brokerSuspendMaxTimeMillis = brokerSuspendMaxTimeMillis;
		}

		public long getConsumerTimeoutMillisWhenSuspend() {
			return consumerTimeoutMillisWhenSuspend;
		}

		public void setConsumerTimeoutMillisWhenSuspend(long consumerTimeoutMillisWhenSuspend) {
			this.consumerTimeoutMillisWhenSuspend = consumerTimeoutMillisWhenSuspend;
		}

		public long getConsumerPullTimeoutMillis() {
			return consumerPullTimeoutMillis;
		}

		public void setConsumerPullTimeoutMillis(long consumerPullTimeoutMillis) {
			this.consumerPullTimeoutMillis = consumerPullTimeoutMillis;
		}

		public MessageQueueListener getMessageQueueListener() {
			return messageQueueListener;
		}

		public void setMessageQueueListener(MessageQueueListener messageQueueListener) {
			this.messageQueueListener = messageQueueListener;
		}

		public boolean getAutoCommit() {
			return autoCommit;
		}

		public void setAutoCommit(boolean autoCommit) {
			this.autoCommit = autoCommit;
		}

		public long getAutoCommitIntervalMillis() {
			return autoCommitIntervalMillis;
		}

		public void setAutoCommitIntervalMillis(long autoCommitIntervalMillis) {
			this.autoCommitIntervalMillis = autoCommitIntervalMillis;
		}

		public long getPullThresholdForAll() {
			return pullThresholdForAll;
		}

		public void setPullThresholdForAll(long pullThresholdForAll) {
			this.pullThresholdForAll = pullThresholdForAll;
		}

		public int getConsumeMaxSpan() {
			return consumeMaxSpan;
		}

		public void setConsumeMaxSpan(int consumeMaxSpan) {
			this.consumeMaxSpan = consumeMaxSpan;
		}

		public long getPollTimeoutMillis() {
			return pollTimeoutMillis;
		}

		public void setPollTimeoutMillis(long pollTimeoutMillis) {
			this.pollTimeoutMillis = pollTimeoutMillis;
		}

		public long getTopicMetadataCheckIntervalMillis() {
			return topicMetadataCheckIntervalMillis;
		}

		public void setTopicMetadataCheckIntervalMillis(long topicMetadataCheckIntervalMillis) {
			this.topicMetadataCheckIntervalMillis = topicMetadataCheckIntervalMillis;
		}
	}

}
