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

package com.zkzlx.stream.rocketmq.support;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.ByteArrayMessageConverter;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.converter.StringMessageConverter;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.MimeTypeUtils;
import org.springframework.util.StringUtils;

import com.zkzlx.stream.rocketmq.contants.RocketMQConst;
import com.zkzlx.stream.rocketmq.contants.RocketMQConst.Headers;

/**
 * TODO Describe what it does
 *
 * @author zkzlx
 */
public class RocketMQMessageConverterSupport {

    private static final boolean JACKSON_PRESENT;
    private static final boolean FASTJSON_PRESENT;

    static {
        ClassLoader classLoader = RocketMQMessageConverterSupport.class.getClassLoader();
        JACKSON_PRESENT =
                ClassUtils.isPresent("com.fasterxml.jackson.databind.ObjectMapper", classLoader) &&
                        ClassUtils.isPresent("com.fasterxml.jackson.core.JsonGenerator", classLoader);
        FASTJSON_PRESENT = ClassUtils.isPresent("com.alibaba.fastjson.JSON", classLoader) &&
                ClassUtils.isPresent("com.alibaba.fastjson.support.config.FastJsonConfig", classLoader);
    }


    private final CompositeMessageConverter messageConverter;

    private RocketMQMessageConverterSupport() {
        List<MessageConverter> messageConverters = new ArrayList<>();
        ByteArrayMessageConverter byteArrayMessageConverter = new ByteArrayMessageConverter();
        byteArrayMessageConverter.setContentTypeResolver(null);
        messageConverters.add(byteArrayMessageConverter);
        messageConverters.add(new StringMessageConverter());
        if (JACKSON_PRESENT) {
            MappingJackson2MessageConverter jackson2MessageConverter= new MappingJackson2MessageConverter();
            messageConverters.add(jackson2MessageConverter);
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


    public Message convertMessage2Spring(MessageExt message) {
        MessageBuilder messageBuilder = MessageBuilder.withPayload(message.getBody())
                .setHeader(toRocketHeaderKey(Headers.KEYS), message.getKeys())
                .setHeader(toRocketHeaderKey(Headers.TAGS), message.getTags())
                .setHeader(toRocketHeaderKey(Headers.TOPIC), message.getTopic())
                .setHeader(toRocketHeaderKey(Headers.MESSAGE_ID),
                        message.getMsgId())
                .setHeader(toRocketHeaderKey(Headers.BORN_TIMESTAMP),
                        message.getBornTimestamp())
                .setHeader(toRocketHeaderKey(Headers.BORN_HOST),
                        message.getBornHostString())
                .setHeader(toRocketHeaderKey(Headers.FLAG), message.getFlag())
                .setHeader(toRocketHeaderKey(Headers.QUEUE_ID),
                        message.getQueueId())
                .setHeader(toRocketHeaderKey(Headers.SYS_FLAG),
                        message.getSysFlag())
                .setHeader(toRocketHeaderKey(Headers.TRANSACTION_ID),
                        message.getTransactionId());
        addUserProperties(message.getProperties(), messageBuilder);
        return messageBuilder.build();
    }

    public static String toRocketHeaderKey(String rawKey) {
        return "ROCKET_" + rawKey;
    }

    private static void addUserProperties(Map<String, String> properties, MessageBuilder messageBuilder) {
        if (!CollectionUtils.isEmpty(properties)) {
            properties.forEach((key, val) -> {
                if (!MessageConst.STRING_HASH_SET.contains(key) && !MessageHeaders.ID.equals(key)
                        && !MessageHeaders.TIMESTAMP.equals(key)) {
                    messageBuilder.setHeader(key, val);
                }
            });
        }
    }

    public org.apache.rocketmq.common.message.Message convertMessage2MQ(String destination, Message<?> source) {
        Message<?> message = messageConverter.toMessage(source.getPayload(), source.getHeaders());
        assert message != null;
        MessageBuilder<?> builder = MessageBuilder.fromMessage(message);
        builder.setHeaderIfAbsent(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN);
        message = builder.build();
        return this.doConvert(destination,message);
    }

    private org.apache.rocketmq.common.message.Message doConvert(String topic, Message<?> message) {
        Charset charset = Charset.defaultCharset();
        Object payloadObj = message.getPayload();
        byte[] payloads;
        try {
            if (null == payloadObj) {
                throw new RuntimeException("the message cannot be empty");
            }
            if (payloadObj instanceof String) {
                payloads = ((String) payloadObj).getBytes(charset);
            } else if (payloadObj instanceof byte[]) {
                payloads = (byte[]) message.getPayload();
            } else {
                String jsonObj = (String) messageConverter.fromMessage(message, payloadObj.getClass());
                if (null == jsonObj) {
                    throw new RuntimeException(String.format(
                            "empty after conversion [messageConverter:%s,payloadClass:%s,payloadObj:%s]",
                            messageConverter.getClass(), payloadObj.getClass(), payloadObj));
                }
                payloads = jsonObj.getBytes(charset);
            }
        } catch (Exception e) {
            throw new RuntimeException("convert to RocketMQ message failed.", e);
        }
        return getAndWrapMessage(topic, message.getHeaders(), payloads);
    }


    private static org.apache.rocketmq.common.message.Message getAndWrapMessage(String topic, MessageHeaders headers, byte[] payloads) {
        if (topic == null || topic.length() < 1) {
            return null;
        }
        if (payloads == null || payloads.length < 1) {
            return null;
        }
        org.apache.rocketmq.common.message.Message rocketMsg = new org.apache.rocketmq.common.message.Message(topic, payloads);
        if (Objects.nonNull(headers) && !headers.isEmpty()) {
            Object tag = headers.getOrDefault(Headers.TAGS, headers.get(toRocketHeaderKey(Headers.TAGS)));
            if (!StringUtils.isEmpty(tag)) {
                rocketMsg.setTags(String.valueOf(tag));
            }

            Object keys = headers.getOrDefault(Headers.KEYS, headers.get(toRocketHeaderKey(Headers.KEYS)));
            if (!StringUtils.isEmpty(keys)) {
                rocketMsg.setKeys(keys.toString());
            }
            Object flagObj = headers.getOrDefault(Headers.FLAG, headers.get(toRocketHeaderKey(Headers.FLAG)));
            int flag = 0;
            int delayLevel = 0;
			try {
				flagObj = flagObj == null ? 0 : flagObj;
				Object delayLevelObj = headers.getOrDefault(
                        RocketMQConst.PROPERTY_DELAY_TIME_LEVEL,
						headers.get(toRocketHeaderKey(RocketMQConst.PROPERTY_DELAY_TIME_LEVEL)));
				delayLevelObj = delayLevelObj == null ? 0 : delayLevelObj;
				delayLevel = Integer.parseInt(String.valueOf(delayLevelObj));
				flag = Integer.parseInt(String.valueOf(flagObj));
			}
            catch (Exception ignored) {
            }
            if (delayLevel > 0) {
                rocketMsg.setDelayTimeLevel(delayLevel);
            }
            rocketMsg.setFlag(flag);
            Object waitStoreMsgOkObj = headers.getOrDefault(RocketMQConst.PROPERTY_WAIT_STORE_MSG_OK, "true");
            rocketMsg.setWaitStoreMsgOK(Boolean.TRUE.equals(waitStoreMsgOkObj));
            headers.entrySet().stream()
                    .filter(entry -> !Objects.equals(entry.getKey(), Headers.FLAG)
                            && !Objects.equals(entry.getKey(), RocketMQConst.PROPERTY_WAIT_STORE_MSG_OK))
                    .forEach(entry -> {
                        if (!MessageConst.STRING_HASH_SET.contains(entry.getKey())) {
                            rocketMsg.putUserProperty(entry.getKey(), String.valueOf(entry.getValue()));
                        }
                    });

        }
        return rocketMsg;
    }


    public MessageConverter getMessageConverter() {
        return messageConverter;
    }

    public static RocketMQMessageConverterSupport instance() {
        return ConverterSupport.INSTANCE;
    }

    private static class ConverterSupport {
        private static final RocketMQMessageConverterSupport INSTANCE = new RocketMQMessageConverterSupport();
    }

}
