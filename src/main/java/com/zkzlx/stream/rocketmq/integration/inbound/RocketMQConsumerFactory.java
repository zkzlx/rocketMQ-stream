package com.zkzlx.stream.rocketmq.integration.inbound;

import com.zkzlx.stream.rocketmq.custom.RocketMQBeanContainerCache;
import com.zkzlx.stream.rocketmq.properties.RocketMQConsumerProperties;
import com.zkzlx.stream.rocketmq.utils.RocketMQUtils;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.RPCHook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Extended function related to producer . eg:initial
 *
 * @author zkzlx
 */
public final class RocketMQConsumerFactory {

	private final static Logger log = LoggerFactory
			.getLogger(RocketMQConsumerFactory.class);

	public static DefaultMQPushConsumer initPushConsumer(
			RocketMQConsumerProperties consumerProperties) {

		Assert.notNull(consumerProperties.getGroup(),
				"Property 'consumerGroup' is required");
		Assert.notNull(consumerProperties.getNameServer(),
				"Property 'nameServer' is required");
		AllocateMessageQueueStrategy allocateMessageQueueStrategy = RocketMQBeanContainerCache
				.getBean(consumerProperties.getAllocateMessageQueueStrategy(),
						AllocateMessageQueueStrategy.class,
						new AllocateMessageQueueAveragely());
		RPCHook rpcHook = null;
		if (!StringUtils.isEmpty(consumerProperties.getAccessKey())
				&& !StringUtils.isEmpty(consumerProperties.getSecretKey())) {
			rpcHook = new AclClientRPCHook(
					new SessionCredentials(consumerProperties.getAccessKey(),
							consumerProperties.getSecretKey()));
		}
		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(
				consumerProperties.getGroup(), rpcHook, allocateMessageQueueStrategy,
				consumerProperties.getEnableMsgTrace(),
				consumerProperties.getCustomizedTraceTopic());
		consumer.setVipChannelEnabled(
				null == rpcHook && consumerProperties.getVipChannelEnabled());
		consumer.setInstanceName(
				RocketMQUtils.getInstanceName(rpcHook, consumerProperties.getGroup()));
		consumer.setNamespace(consumerProperties.getNamespace());
		consumer.setNamesrvAddr(consumerProperties.getNameServer());
		consumer.setMessageModel(getMessageModel(consumerProperties.getMessageModel()));

		return consumer;
	}

	public static DefaultLitePullConsumer initPullConsumer(
			RocketMQConsumerProperties consumerProperties) {
		Assert.notNull(consumerProperties.getGroup(),
				"Property 'consumerGroup' is required");
		Assert.notNull(consumerProperties.getNameServer(),
				"Property 'nameServer' is required");
		AllocateMessageQueueStrategy allocateMessageQueueStrategy = RocketMQBeanContainerCache
				.getBean(consumerProperties.getAllocateMessageQueueStrategy(),
						AllocateMessageQueueStrategy.class,
						new AllocateMessageQueueAveragely());

		RPCHook rpcHook = null;
		if (!StringUtils.isEmpty(consumerProperties.getAccessKey())
				&& !StringUtils.isEmpty(consumerProperties.getSecretKey())) {
			rpcHook = new AclClientRPCHook(
					new SessionCredentials(consumerProperties.getAccessKey(),
							consumerProperties.getSecretKey()));
		}

		DefaultLitePullConsumer consumer = new DefaultLitePullConsumer(
				consumerProperties.getNamespace(), consumerProperties.getGroup(),
				rpcHook);
		consumer.setVipChannelEnabled(
				null == rpcHook && consumerProperties.getVipChannelEnabled());
		consumer.setInstanceName(
				RocketMQUtils.getInstanceName(rpcHook, consumerProperties.getGroup()));
		consumer.setAllocateMessageQueueStrategy(allocateMessageQueueStrategy);
		consumer.setNamesrvAddr(consumerProperties.getNameServer());
		consumer.setMessageModel(getMessageModel(consumerProperties.getMessageModel()));
		return consumer;
	}

	private static MessageModel getMessageModel(String messageModel) {
		for (MessageModel model : MessageModel.values()) {
			if (model.getModeCN().equalsIgnoreCase(messageModel)) {
				return model;
			}
		}
		return MessageModel.CLUSTERING;
	}

}
