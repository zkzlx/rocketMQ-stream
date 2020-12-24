package com.zkzlx.stream.rocketmq.integration.inbound.poll;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.util.Assert;

/**
 * TODO Describe what it does
 * @author zkzlx
 */
public class RocketMQAckCallback implements AcknowledgmentCallback {
	private final static Logger log = LoggerFactory
			.getLogger(RocketMQAckCallback.class);
	private boolean acknowledged;

	private boolean autoAckEnabled = true;
	private MessageExt messageExt;

	public RocketMQAckCallback(MessageExt messageExt) {
		this.messageExt = messageExt;
	}

	protected void setAcknowledged(boolean acknowledged) {
		this.acknowledged = acknowledged;
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
		try {
			switch (status) {
				case ACCEPT:
				case REJECT:
					log.info("`````````````````````````````````````````success`````````````````````````````````````");
					break;
				case REQUEUE:
					log.info("````````````````````````````````````````re consumer `````````````````````````````````");
					break;
				default:
					break;
			}
		} finally {
			this.acknowledged = true;
		}
	}


}