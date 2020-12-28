package com.zkzlx.stream.rocketmq.autoconfigurate;

import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.context.properties.source.ConfigurationPropertyName;
import org.springframework.cloud.stream.config.BindingHandlerAdvise.MappingsProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ExtendedBindingHandlerMappingsProviderConfiguration {

	@Bean
	public MappingsProvider rocketExtendedPropertiesDefaultMappingsProvider() {
		return () -> {
			Map<ConfigurationPropertyName, ConfigurationPropertyName> mappings = new HashMap<>();
			mappings.put(
					ConfigurationPropertyName.of("spring.cloud.stream.rocketmq.bindings"),
					ConfigurationPropertyName.of("spring.cloud.stream.rocketmq.default"));
			mappings.put(
					ConfigurationPropertyName.of("spring.cloud.stream.rocketmq.streams"),
					ConfigurationPropertyName
							.of("spring.cloud.stream.rocketmq.streams.default"));
			return mappings;
		};
	}

}
