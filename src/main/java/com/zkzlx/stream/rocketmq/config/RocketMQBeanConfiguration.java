package com.zkzlx.stream.rocketmq.config;

import com.zkzlx.stream.rocketmq.custom.RocketMQConfigBeanPostProcessor;
import com.zkzlx.stream.rocketmq.properties.RocketMQConsumerProperties;
import com.zkzlx.stream.rocketmq.properties.RocketMQProducerProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

/**
 * RocketMQ BeanPOstProcessor init
 *
 * @author junboXiang
 */
@Component
@EnableConfigurationProperties(value = {RocketMQProducerProperties.class, RocketMQConsumerProperties.class})
public class RocketMQBeanConfiguration {

    @Bean
    public RocketMQConfigBeanPostProcessor initRocketMQConfigBeanPostProcessor() {
        return new RocketMQConfigBeanPostProcessor();
    }

}
