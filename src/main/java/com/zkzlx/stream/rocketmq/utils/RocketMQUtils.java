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

package com.zkzlx.stream.rocketmq.utils;

import java.util.List;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.MessageSelector;
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

import javafx.util.Pair;

/**
 * TODO Describe what it does
 *
 * @author zkzlx
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

	public static MessageSelector getMessageSelector(String expression){
		if(StringUtils.hasText(expression) && !expression.contains("||")){
			return MessageSelector.bySql(expression);
		}
		return MessageSelector.byTag(expression);
	}



}
