
package com.zkzlx.stream.rocketmq.properties;

import org.springframework.cloud.stream.binder.BinderSpecificPropertiesProvider;

/**
 * Container object for RocketMQ specific extended producer and consumer binding
 * properties.
 *
 * @author zkzlx
 */
public class RocketMQSpecificPropertiesProvider
		implements BinderSpecificPropertiesProvider {

	/**
	 * Consumer specific binding properties. @see {@link RocketMQConsumerProperties}.
	 */
	private RocketMQConsumerProperties consumer = new RocketMQConsumerProperties();

	/**
	 * Producer specific binding properties. @see {@link RocketMQProducerProperties}.
	 */
	private RocketMQProducerProperties producer = new RocketMQProducerProperties();

	/**
	 * @return {@link RocketMQConsumerProperties} Consumer specific binding
	 * properties. @see {@link RocketMQConsumerProperties}.
	 */
	@Override
	public RocketMQConsumerProperties getConsumer() {
		return this.consumer;
	}

	public void setConsumer(RocketMQConsumerProperties consumer) {
		this.consumer = consumer;
	}

	/**
	 * @return {@link RocketMQProducerProperties} Producer specific binding
	 * properties. @see {@link RocketMQProducerProperties}.
	 */
	@Override
	public RocketMQProducerProperties getProducer() {
		return this.producer;
	}

	public void setProducer(RocketMQProducerProperties producer) {
		this.producer = producer;
	}

}
