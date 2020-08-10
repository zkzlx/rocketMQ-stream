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

package com.zkzlx.stream.rocketmq;

import com.zkzlx.stream.rocketmq.handler.RocketMQMessageHandler;
import com.zkzlx.stream.rocketmq.integration.RocketMQMessageSource;
import com.zkzlx.stream.rocketmq.properties.RocketMQBinderConfigurationProperties;
import com.zkzlx.stream.rocketmq.properties.RocketMQConsumerProperties;
import com.zkzlx.stream.rocketmq.properties.RocketMQExtendedBindingProperties;
import com.zkzlx.stream.rocketmq.properties.RocketMQProducerProperties;
import com.zkzlx.stream.rocketmq.provisioning.RocketMQTopicProvisioner;

import org.springframework.cloud.stream.binder.AbstractMessageChannelBinder;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.BinderSpecificPropertiesProvider;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.cloud.stream.config.ListenerContainerCustomizer;
import org.springframework.cloud.stream.config.MessageSourceCustomizer;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.context.ApplicationContext;
import org.springframework.context.Lifecycle;
import org.springframework.integration.amqp.inbound.AmqpMessageSource;
import org.springframework.integration.core.MessageProducer;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.util.Assert;

/**
 * A {@link org.springframework.cloud.stream.binder.Binder} that uses RocketMQ as the
 * underlying middleware.
 *
 * @author zkzlx
 */
public class RocketMQMessageChannelBinder extends
		AbstractMessageChannelBinder<ExtendedConsumerProperties<RocketMQConsumerProperties>, ExtendedProducerProperties<RocketMQProducerProperties>, RocketMQTopicProvisioner>
		implements
		ExtendedPropertiesBinder<MessageChannel, RocketMQConsumerProperties, RocketMQProducerProperties> {

	private RocketMQExtendedBindingProperties extendedBindingProperties = new RocketMQExtendedBindingProperties();

	private RocketMQBinderConfigurationProperties binderConfigurationProperties;

	public RocketMQMessageChannelBinder(String[] headersToEmbed,
			RocketMQTopicProvisioner provisioningProvider) {
		super(headersToEmbed, provisioningProvider);
	}

	public RocketMQMessageChannelBinder(String[] headersToEmbed,
			RocketMQTopicProvisioner provisioningProvider,
			ListenerContainerCustomizer<?> containerCustomizer,
			MessageSourceCustomizer<?> sourceCustomizer) {
		super(headersToEmbed, provisioningProvider, containerCustomizer,
				sourceCustomizer);
	}

	/**
	 * Create a {@link MessageHandler} with the ability to send data to the target
	 * middleware. If the returned instance is also a {@link Lifecycle}, it will be
	 * stopped automatically by the binder.
	 * <p>
	 * In order to be fully compliant, the {@link MessageHandler} of the binder must
	 * observe the following headers:
	 * <ul>
	 * <li>{@link BinderHeaders#PARTITION_HEADER} - indicates the target partition where
	 * the message must be sent</li>
	 * </ul>
	 * <p>
	 *
	 * @param destination        the name of the target destination.
	 * @param producerProperties the producer properties.
	 * @param channel            the channel to bind.
	 * @param errorChannel       the error channel (if enabled, otherwise null). If not null,
	 *                           the binder must wire this channel into the producer endpoint so that errors are
	 *                           forwarded to it.
	 * @return the message handler for sending data to the target middleware
	 * @throws Exception when producer messsage handler failed to be created
	 */
	@Override
	protected MessageHandler createProducerMessageHandler(ProducerDestination destination
			, ExtendedProducerProperties<RocketMQProducerProperties> producerProperties
			, MessageChannel channel, MessageChannel errorChannel) throws Exception {
//		if (!producerProperties.getExtension().getEnable()) {
//			throw new RuntimeException("Binding for channel " + destination.getName()
//					+ " has been disabled, message can't be delivered");
//		}
		RocketMQMessageHandler messageHandler = new RocketMQMessageHandler(binderConfigurationProperties,producerProperties,destination);
		messageHandler.setApplicationContext(this.getApplicationContext());
		if (errorChannel != null) {
			messageHandler.setSendFailureChannel(errorChannel);
		}
		messageHandler.setBeanFactory(this.getApplicationContext().getBeanFactory());
		return messageHandler;
	}

	@Override
	protected MessageHandler createProducerMessageHandler(ProducerDestination destination,
			ExtendedProducerProperties<RocketMQProducerProperties> producerProperties,
			MessageChannel errorChannel) throws Exception {
		throw new UnsupportedOperationException(
				"The abstract binder should not call this method");
	}

	/**
	 * Creates {@link MessageProducer} that receives data from the consumer destination.
	 * will be started and stopped by the binder.
	 *
	 * @param destination reference to the consumer destination
	 * @param group the consumer group
	 * @param properties the consumer properties
	 * @return the consumer endpoint.
	 * @throws Exception when consumer endpoint creation failed.
	 */
	@Override
	protected MessageProducer createConsumerEndpoint(ConsumerDestination destination,
			String group,
			ExtendedConsumerProperties<RocketMQConsumerProperties> properties)
			throws Exception {
		return null;
	}

	/**
	 *  the consumer of long polling mode .
	 * @param name
	 * @param group
	 * @param destination
	 * @param consumerProperties
	 * @return
	 */
	@Override
	protected PolledConsumerResources createPolledConsumerResources(String name, String group, ConsumerDestination destination
			, ExtendedConsumerProperties<RocketMQConsumerProperties> consumerProperties) {

		RocketMQMessageSource messageSource = new RocketMQMessageSource(
				binderConfigurationProperties, consumerProperties, name, group);
		return new PolledConsumerResources(messageSource,
				registerErrorInfrastructure(destination, group, consumerProperties,
						true));
	}

	@Override
	public RocketMQConsumerProperties getExtendedConsumerProperties(String channelName) {
		return this.extendedBindingProperties.getExtendedConsumerProperties(channelName);
	}

	@Override
	public RocketMQProducerProperties getExtendedProducerProperties(String channelName) {
		return this.extendedBindingProperties.getExtendedProducerProperties(channelName);
	}

	/**
	 * Extended binding properties can define a default prefix to place all the extended
	 * common producer and consumer properties. For example, if the binder type is foo it
	 * is convenient to specify common extended properties for the producer or consumer
	 * across multiple bindings in the form of
	 * `spring.cloud.stream.foo.default.producer.x=y` or
	 * `spring.cloud.stream.foo.default.consumer.x=y`.
	 * <p>
	 * The binding process will use this defaults prefix to resolve any common extended
	 * producer and consumer properties.
	 *
	 * @return default prefix for extended properties
	 * @since 2.1.0
	 */
	@Override
	public String getDefaultsPrefix() {
		return this.extendedBindingProperties.getDefaultsPrefix();
	}

	/**
	 * Extended properties class which should be a subclass of
	 * {@link BinderSpecificPropertiesProvider} against which default extended producer
	 * and consumer properties are resolved.
	 *
	 * @return extended properties class that contains extended producer/consumer
	 * properties
	 * @since 2.1.0
	 */
	@Override
	public Class<? extends BinderSpecificPropertiesProvider> getExtendedPropertiesEntryClass() {
		return this.extendedBindingProperties.getExtendedPropertiesEntryClass();
	}
}
