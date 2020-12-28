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
import com.zkzlx.stream.rocketmq.actuator.RocketMQBinderHealthIndicator;
import com.zkzlx.stream.rocketmq.convert.RocketMQMessageConverter;
import com.zkzlx.stream.rocketmq.custom.RocketMQConfigBeanPostProcessor;
import com.zkzlx.stream.rocketmq.properties.RocketMQBinderConfigurationProperties;
import com.zkzlx.stream.rocketmq.properties.RocketMQExtendedBindingProperties;
import com.zkzlx.stream.rocketmq.provisioning.RocketMQTopicProvisioner;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.autoconfigure.health.ConditionalOnEnabledHealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Timur Valiev
 * @author <a href="mailto:fangjian0423@gmail.com">Jim</a>
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties({ RocketMQExtendedBindingProperties.class,
		RocketMQBinderConfigurationProperties.class })
public class RocketMQBinderAutoConfiguration {

	@Autowired
	private RocketMQExtendedBindingProperties extendedBindingProperties;
	@Autowired
	private RocketMQBinderConfigurationProperties rocketBinderConfigurationProperties;

	@Bean
	public RocketMQConfigBeanPostProcessor initRocketMQConfigBeanPostProcessor() {
		return new RocketMQConfigBeanPostProcessor();
	}

	@Bean(RocketMQMessageConverter.DEFAULT_NAME)
	@ConditionalOnMissingBean(name = {RocketMQMessageConverter.DEFAULT_NAME})
	public RocketMQMessageConverter initRocketMQMessageConverter() {
		return new RocketMQMessageConverter();
	}

	@Bean
	@ConditionalOnEnabledHealthIndicator("rocketmq")
	@ConditionalOnClass(name = "org.springframework.boot.actuate.health.HealthIndicator")
	public RocketMQBinderHealthIndicator rocketMQBinderHealthIndicator() {
		return new RocketMQBinderHealthIndicator();
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


	@Bean
	public MyEndPoint myEndPoint(){
		return new MyEndPoint();
	}
}
