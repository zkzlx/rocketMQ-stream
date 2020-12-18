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

import com.zkzlx.stream.rocketmq.properties.RocketMQBinderConfigurationProperties;
import com.zkzlx.stream.rocketmq.properties.RocketMQCommonProperties;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.remoting.RPCHook;

import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * TODO Describe what it does
 *
 * @author zkzlx
 */
public class RocketMQUtils {

	public static <T extends RocketMQCommonProperties> T mergeRocketMQProperties(
			RocketMQBinderConfigurationProperties binderConfigurationProperties,T mqProperties) {
		if (null == binderConfigurationProperties) {
			return mqProperties;
		}
		if (StringUtils.isEmpty(mqProperties.getNameServer())) {
			mqProperties
					.setNameServer(binderConfigurationProperties.getNameServer());
		}
		if (StringUtils.isEmpty(mqProperties.getSecretKey())) {
			mqProperties.setSecretKey(binderConfigurationProperties.getSecretKey());
		}
		if (StringUtils.isEmpty(mqProperties.getAccessKey())) {
			mqProperties.setAccessKey(binderConfigurationProperties.getAccessKey());
		}
		if (StringUtils.isEmpty(mqProperties.getAccessChannel())) {
			mqProperties
					.setAccessChannel(binderConfigurationProperties.getAccessChannel());
		}
		if (StringUtils.isEmpty(mqProperties.getNamespace())) {
			mqProperties.setNamespace(binderConfigurationProperties.getNamespace());
		}
		if (StringUtils.isEmpty(mqProperties.getGroup())) {
			mqProperties.setGroup(binderConfigurationProperties.getGroup());
		}
		if (StringUtils.isEmpty(mqProperties.getCustomizedTraceTopic())) {
			mqProperties.setCustomizedTraceTopic(
					binderConfigurationProperties.getCustomizedTraceTopic());
		}
		mqProperties.setNameServer(getNameServerStr(mqProperties.getNameServer()));
		return mqProperties;
	}

	public static String getInstanceName(RPCHook rpcHook, String identify) {
		String separator = "|";
		StringBuilder instanceName = new StringBuilder();
		if (null != rpcHook) {
			SessionCredentials sessionCredentials = ((AclClientRPCHook) rpcHook)
					.getSessionCredentials();
			instanceName.append(sessionCredentials.getAccessKey()).append(separator)
					.append(sessionCredentials.getSecretKey()).append(separator);
		}
		instanceName.append(identify).append(separator).append(UtilAll.getPid());
		return instanceName.toString();
	}

	public static String getNameServerStr(List<String> nameServerList) {
		if (CollectionUtils.isEmpty(nameServerList)) {
			return null;
		}
		return String.join(";", nameServerList);
	}
	public static String getNameServerStr(String nameServer) {
		if (StringUtils.isEmpty(nameServer)) {
			return null;
		}
		return nameServer.replaceAll(",",";");
	}


	public static MessageSelector getMessageSelector(String expression) {
		if (StringUtils.hasText(expression) && !expression.contains("||")) {
			return MessageSelector.bySql(expression);
		}
		return MessageSelector.byTag(expression);
	}

}
