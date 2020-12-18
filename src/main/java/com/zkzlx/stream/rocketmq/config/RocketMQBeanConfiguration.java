package com.zkzlx.stream.rocketmq.config;

import com.zkzlx.stream.rocketmq.custom.RocketMQConfigBeanPostProcessor;
import com.zkzlx.stream.rocketmq.properties.RocketMQConsumerProperties;
import com.zkzlx.stream.rocketmq.properties.RocketMQProducerProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

/**
 * RocketMQ BeanPOstProcessor init
 *
 * @author junboXiang
 */
@Configuration
public class RocketMQBeanConfiguration {

    @Bean
    public RocketMQConfigBeanPostProcessor initRocketMQConfigBeanPostProcessor() {
        return new RocketMQConfigBeanPostProcessor();
    }

}
