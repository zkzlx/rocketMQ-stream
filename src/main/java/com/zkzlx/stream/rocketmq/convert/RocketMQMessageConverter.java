package com.zkzlx.stream.rocketmq.convert;

import java.util.ArrayList;
import java.util.List;

import org.springframework.messaging.converter.ByteArrayMessageConverter;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.converter.StringMessageConverter;
import org.springframework.util.ClassUtils;

/**
 * The default message converter of rocketMq,its bean name is {@link #DEFAULT_NAME}
 * @author zkzlx
 */
public class RocketMQMessageConverter {

    public static final String DEFAULT_NAME = "rocketMQMessageConverter";

    private static final boolean JACKSON_PRESENT;
    private static final boolean FASTJSON_PRESENT;

    static {
        ClassLoader classLoader = RocketMQMessageConverter.class.getClassLoader();
        JACKSON_PRESENT =
                ClassUtils.isPresent("com.fasterxml.jackson.databind.ObjectMapper", classLoader) &&
                        ClassUtils.isPresent("com.fasterxml.jackson.core.JsonGenerator", classLoader);
        FASTJSON_PRESENT = ClassUtils.isPresent("com.alibaba.fastjson.JSON", classLoader) &&
                ClassUtils.isPresent("com.alibaba.fastjson.support.config.FastJsonConfig", classLoader);
    }


    private CompositeMessageConverter messageConverter;

    public RocketMQMessageConverter() {
        List<MessageConverter> messageConverters = new ArrayList<>();
        ByteArrayMessageConverter byteArrayMessageConverter = new ByteArrayMessageConverter();
        byteArrayMessageConverter.setContentTypeResolver(null);
        messageConverters.add(byteArrayMessageConverter);
        messageConverters.add(new StringMessageConverter());
        if (JACKSON_PRESENT) {
            messageConverters.add(new MappingJackson2MessageConverter());
        }
        if (FASTJSON_PRESENT) {
            try {
                messageConverters.add(
                        (MessageConverter) ClassUtils.forName(
                                "com.alibaba.fastjson.support.spring.messaging.MappingFastJsonMessageConverter",
                                ClassUtils.getDefaultClassLoader()).newInstance());
            } catch (ClassNotFoundException | IllegalAccessException | InstantiationException ignored) {
                //ignore this exception
            }
        }
        messageConverter = new CompositeMessageConverter(messageConverters);
    }

    public CompositeMessageConverter getMessageConverter() {
        return messageConverter;
    }

    public void setMessageConverter(CompositeMessageConverter messageConverter) {
        this.messageConverter = messageConverter;
    }
}