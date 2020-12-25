/*
 * Copyright 2013-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zkzlx.stream.rocketmq.autoconfigurate;

import com.zkzlx.stream.rocketmq.RocketMQMessageChannelBinder;
import com.zkzlx.stream.rocketmq.config.RocketMQBeanConfiguration;
import com.zkzlx.stream.rocketmq.properties.RocketMQBinderConfigurationProperties;
import com.zkzlx.stream.rocketmq.properties.RocketMQExtendedBindingProperties;
import com.zkzlx.stream.rocketmq.provisioning.RocketMQTopicProvisioner;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * @author Timur Valiev
 * @author zkzlx
 */
@Configuration(proxyBeanMethods = false)
@Import({ RocketMQBeanConfiguration.class })
@EnableConfigurationProperties({ RocketMQExtendedBindingProperties.class,
		RocketMQBinderConfigurationProperties.class })
public class RocketMQBinderAutoConfiguration {

	private final RocketMQExtendedBindingProperties extendedBindingProperties;
	private final RocketMQBinderConfigurationProperties rocketBinderConfigurationProperties;

	@Autowired
	public RocketMQBinderAutoConfiguration(
			RocketMQExtendedBindingProperties extendedBindingProperties,
			RocketMQBinderConfigurationProperties rocketBinderConfigurationProperties) {
		this.extendedBindingProperties = extendedBindingProperties;
		this.rocketBinderConfigurationProperties = rocketBinderConfigurationProperties;
	}

	@Bean
	public RocketMQTopicProvisioner provisioningProvider() {
		return new RocketMQTopicProvisioner();
	}

	@Bean
	public RocketMQMessageChannelBinder rocketMessageChannelBinder(
			RocketMQTopicProvisioner provisioningProvider) {
		return new RocketMQMessageChannelBinder(rocketBinderConfigurationProperties,extendedBindingProperties,
				provisioningProvider);
	}

}
