package com.zkzlx.stream.rocketmq.utils;

import java.util.List;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MQProducer;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.remoting.RPCHook;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import com.zkzlx.stream.rocketmq.properties.RocketMQBinderConfigurationProperties;
import com.zkzlx.stream.rocketmq.properties.RocketMQExtendedBindingProperties;
import com.zkzlx.stream.rocketmq.properties.RocketMQProducerProperties;
import com.zkzlx.stream.rocketmq.properties.RocketMQProducerProperties.ProducerType;

/**
 * TODO Describe what it does
 *
 * @author zkz
 */
public class RocketMQUtils {

	public static RocketMQProducerProperties mergeRocketMQProducerProperties(
			RocketMQBinderConfigurationProperties binderConfigurationProperties,
			RocketMQProducerProperties producerProperties) {
		if (producerProperties == null && binderConfigurationProperties == null) {
			return new RocketMQProducerProperties();
		}
		if (producerProperties == null) {
			producerProperties = new RocketMQProducerProperties();
		}
		if (null == binderConfigurationProperties) {
			return producerProperties;
		}
		if (StringUtils.isEmpty(producerProperties.getNameServer())) {
			producerProperties
					.setNameServer(binderConfigurationProperties.getNameServer());
		}
		if (StringUtils.isEmpty(producerProperties.getSecretKey())) {
			producerProperties.setSecretKey(binderConfigurationProperties.getSecretKey());
		}
		if (StringUtils.isEmpty(producerProperties.getAccessKey())) {
			producerProperties.setAccessKey(binderConfigurationProperties.getAccessKey());
		}
		if (StringUtils.isEmpty(producerProperties.getAccessChannel())) {
			producerProperties
					.setAccessChannel(binderConfigurationProperties.getAccessChannel());
		}
		if (StringUtils.isEmpty(producerProperties.getNamespace())) {
			producerProperties.setNamespace(binderConfigurationProperties.getNamespace());
		}
		if (StringUtils.isEmpty(producerProperties.getGroup())) {
			producerProperties.setGroup(binderConfigurationProperties.getGroup());
		}
		if (StringUtils.isEmpty(producerProperties.getCustomizedTraceTopic())) {
			producerProperties.setCustomizedTraceTopic(
					binderConfigurationProperties.getCustomizedTraceTopic());
		}

		return producerProperties;
	}

	public static String getInstanceName(RPCHook rpcHook, String identify) {
		String separator = "|";
		StringBuilder instanceName = new StringBuilder();
		SessionCredentials sessionCredentials = ((AclClientRPCHook) rpcHook)
				.getSessionCredentials();
		instanceName.append(sessionCredentials.getAccessKey()).append(separator)
				.append(sessionCredentials.getSecretKey()).append(separator)
				.append(identify).append(separator).append(UtilAll.getPid());
		return instanceName.toString();
	}

	public static String getNameServerStr(List<String> nameServerList) {
		if (CollectionUtils.isEmpty(nameServerList)) {
			return null;
		}
		return String.join(";", nameServerList);
	}

}
