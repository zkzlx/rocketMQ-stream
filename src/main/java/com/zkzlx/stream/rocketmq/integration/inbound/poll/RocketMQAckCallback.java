package com.zkzlx.stream.rocketmq.integration.inbound.poll;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.consumer.AssignedMessageQueue;
import org.apache.rocketmq.client.impl.consumer.ProcessQueue;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.util.Assert;

/**
 * A pollable {@link org.springframework.integration.core.MessageSource} for RocketMQ.
 * @author zkzlx
 */
public class RocketMQAckCallback implements AcknowledgmentCallback {
	private final static Logger log = LoggerFactory
			.getLogger(RocketMQAckCallback.class);
	private boolean acknowledged;
	private boolean autoAckEnabled = true;
	private MessageExt messageExt;
	private AssignedMessageQueue assignedMessageQueue;
	private DefaultLitePullConsumer consumer;
	private final MessageQueue messageQueue;

	public RocketMQAckCallback(DefaultLitePullConsumer consumer, AssignedMessageQueue assignedMessageQueue,MessageQueue messageQueue, MessageExt messageExt) {
		this.messageExt = messageExt;
		this.consumer = consumer;
		this.assignedMessageQueue = assignedMessageQueue;
		this.messageQueue = messageQueue;
	}

	@Override
	public boolean isAcknowledged() {
		return this.acknowledged;
	}

	@Override
	public void noAutoAck() {
		this.autoAckEnabled = false;
	}

	@Override
	public boolean isAutoAck() {
		return this.autoAckEnabled;
	}

	@Override
	public void acknowledge(Status status) {
		Assert.notNull(status, "'status' cannot be null");
		if (this.acknowledged) {
			throw new IllegalStateException("Already acknowledged");
		}
		synchronized (messageQueue){
			try {
				long offset = messageExt.getQueueOffset();
				switch (status) {
					case REJECT:
					case ACCEPT:
						long consumerOffset = assignedMessageQueue.getConsumerOffset(messageQueue);
						if (consumerOffset != -1) {
							ProcessQueue processQueue = assignedMessageQueue.getProcessQueue(messageQueue);
							if (processQueue != null && !processQueue.isDropped()) {
								consumer.getOffsetStore().updateOffset(messageQueue, consumerOffset, false);
							}
						}
						if (consumer.getMessageModel() == MessageModel.BROADCASTING) {
							consumer.getOffsetStore().persist(messageQueue);
						}
						break;
					case REQUEUE:
						consumer.seek(messageQueue,offset);
						break;
					default:
						break;
				}
			} catch (MQClientException e) {
				throw new IllegalStateException(e);
			} finally {
				this.acknowledged = true;
			}
		}
	}

}