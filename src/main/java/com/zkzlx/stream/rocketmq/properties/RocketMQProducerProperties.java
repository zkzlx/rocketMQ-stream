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

/**
 * Extended producer properties for RocketMQ binder.
 *
 * @author zkzlx
 */
public class RocketMQProducerProperties extends RocketMQCommonProperties {

	/**
	 * Timeout for sending messages.
	 */
	private int sendMsgTimeout = 3000;

	/**
	 * Compress message body threshold, namely, message body larger than 4k will be
	 * compressed on default.
	 */
	private int compressMsgBodyThreshold = 1024 * 4;

	/**
	 * Maximum number of retry to perform internally before claiming sending failure in
	 * synchronous mode.
	 * </p>
	 *
	 * This may potentially cause message duplication which is up to application
	 * developers to resolve.
	 */
	private int retryTimesWhenSendFailed = 2;

	/**
	 * Maximum number of retry to perform internally before claiming sending failure in
	 * asynchronous mode.
	 * </p>
	 *
	 * This may potentially cause message duplication which is up to application
	 * developers to resolve.
	 */
	private int retryTimesWhenSendAsyncFailed = 2;

	/**
	 * Indicate whether to retry another broker on sending failure internally.
	 */
	private boolean retryAnotherBroker = false;

	/**
	 * Maximum allowed message size in bytes.
	 */
	private int maxMessageSize = 1024 * 1024 * 4;

	private String producerType = ProducerType.Normal.name();

	private String sendType = SendType.Sync.name();

	private String sendCallBack;

	private String messageQueueSelector;

	public int getSendMsgTimeout() {
		return sendMsgTimeout;
	}

	public void setSendMsgTimeout(int sendMsgTimeout) {
		this.sendMsgTimeout = sendMsgTimeout;
	}

	public int getCompressMsgBodyThreshold() {
		return compressMsgBodyThreshold;
	}

	public void setCompressMsgBodyThreshold(int compressMsgBodyThreshold) {
		this.compressMsgBodyThreshold = compressMsgBodyThreshold;
	}

	public int getRetryTimesWhenSendFailed() {
		return retryTimesWhenSendFailed;
	}

	public void setRetryTimesWhenSendFailed(int retryTimesWhenSendFailed) {
		this.retryTimesWhenSendFailed = retryTimesWhenSendFailed;
	}

	public int getRetryTimesWhenSendAsyncFailed() {
		return retryTimesWhenSendAsyncFailed;
	}

	public void setRetryTimesWhenSendAsyncFailed(int retryTimesWhenSendAsyncFailed) {
		this.retryTimesWhenSendAsyncFailed = retryTimesWhenSendAsyncFailed;
	}

	public boolean isRetryAnotherBroker() {
		return retryAnotherBroker;
	}

	public void setRetryAnotherBroker(boolean retryAnotherBroker) {
		this.retryAnotherBroker = retryAnotherBroker;
	}

	public int getMaxMessageSize() {
		return maxMessageSize;
	}

	public void setMaxMessageSize(int maxMessageSize) {
		this.maxMessageSize = maxMessageSize;
	}

	public String getProducerType() {
		return producerType;
	}

	public void setProducerType(String producerType) {
		this.producerType = producerType;
	}

	public String getSendType() {
		return sendType;
	}

	public void setSendType(String sendType) {
		this.sendType = sendType;
	}

	public String getSendCallBack() {
		return sendCallBack;
	}

	public void setSendCallBack(String sendCallBack) {
		this.sendCallBack = sendCallBack;
	}

	public String getMessageQueueSelector() {
		return messageQueueSelector;
	}

	public void setMessageQueueSelector(String messageQueueSelector) {
		this.messageQueueSelector = messageQueueSelector;
	}

	public enum ProducerType {
		Normal, Trans;

		public boolean equalsName(String name) {
			return this.name().equalsIgnoreCase(name);
		}
	}

	public enum SendType {
		OneWay, Async, Sync,;

		public boolean equalsName(String name) {
			return this.name().equalsIgnoreCase(name);
		}
	}

}
