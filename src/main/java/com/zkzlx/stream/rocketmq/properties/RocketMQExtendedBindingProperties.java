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

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.stream.binder.AbstractExtendedBindingProperties;
import org.springframework.cloud.stream.binder.BinderSpecificPropertiesProvider;

/**
 * rocketMQ specific extended binding properties class that extends from
 * {@link AbstractExtendedBindingProperties}.
 *
 * @author zkzlx
 */
@ConfigurationProperties("spring.cloud.stream.rocketmq")
public class RocketMQExtendedBindingProperties extends
		AbstractExtendedBindingProperties<RocketMQConsumerProperties, RocketMQProducerProperties, RocketMQSpecificPropertiesProvider> {

	private static final String DEFAULTS_PREFIX = "spring.cloud.stream.rocketmq.default";

	@Override
	public String getDefaultsPrefix() {
		return DEFAULTS_PREFIX;
	}


	@Override
	public Class<? extends BinderSpecificPropertiesProvider> getExtendedPropertiesEntryClass() {
		return RocketMQSpecificPropertiesProvider.class;
	}

}
