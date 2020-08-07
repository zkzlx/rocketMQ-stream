package com.zkzlx.stream.rocketmq.support;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.rocketmq.common.message.MessageConst;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.ByteArrayMessageConverter;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.converter.StringMessageConverter;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.ClassUtils;
import org.springframework.util.MimeTypeUtils;
import org.springframework.util.StringUtils;

import com.zkzlx.stream.rocketmq.contants.RocketMQConst;

/**
 * TODO Describe what it does
 *
 * @author zkz
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


    public org.apache.rocketmq.common.message.Message convertMQMessage(String destination,Message<?> source) {
        Message<?> message = messageConverter.toMessage(source.getPayload(), source.getHeaders());
        MessageBuilder<?> builder = MessageBuilder.fromMessage(message);
        builder.setHeaderIfAbsent(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN);
        message = builder.build();

        return this.doConvert(destination,message);
    }

    private org.apache.rocketmq.common.message.Message doConvert(String destination, Message<?> message) {
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
        return getAndWrapMessage(destination, message.getHeaders(), payloads);
    }


    private static org.apache.rocketmq.common.message.Message getAndWrapMessage(String destination, MessageHeaders headers, byte[] payloads) {
        if (destination == null || destination.length() < 1) {
            return null;
        }
        if (payloads == null || payloads.length < 1) {
            return null;
        }
        String[] tempArr = destination.split(":", 2);
        String topic = tempArr[0];
        String tags = "";
        if (tempArr.length > 1) {
            tags = tempArr[1];
        }
        org.apache.rocketmq.common.message.Message rocketMsg = new org.apache.rocketmq.common.message.Message(topic, tags, payloads);
        if (Objects.nonNull(headers) && !headers.isEmpty()) {
            Object tag = headers.getOrDefault(RocketMQConst.PROPERTY_TAGS, "");
            if (!StringUtils.isEmpty(tag)) {
                rocketMsg.setTags(String.join("||", tag.toString(),tags));
            }

            Object keys = headers.get(RocketMQConst.PROPERTY_KEYS);
            if (!StringUtils.isEmpty(keys)) {
                rocketMsg.setKeys(keys.toString());
            }
            Object flagObj = headers.getOrDefault("FLAG", "0");
            int flag = 0;
            int delayLevel = 0;
            try {
                Object delayLevelObj = headers.get(RocketMQConst.PROPERTY_SELECTOR_ARGS);
                if (delayLevelObj instanceof Number) {
                    delayLevel = ((Number) delayLevelObj).intValue();
                } else if (delayLevelObj instanceof String) {
                    delayLevel = Integer.parseInt((String) delayLevelObj);
                }
                flag = Integer.parseInt(flagObj.toString());
            } catch (NumberFormatException ignored) {
            }
            if (delayLevel > 0) {
                rocketMsg.setDelayTimeLevel(delayLevel);
            }
            rocketMsg.setFlag(flag);
            Object waitStoreMsgOkObj = headers.getOrDefault(RocketMQConst.PROPERTY_WAIT_STORE_MSG_OK, "true");
            rocketMsg.setWaitStoreMsgOK(Boolean.TRUE.equals(waitStoreMsgOkObj));
            headers.entrySet().stream()
                    .filter(entry -> !Objects.equals(entry.getKey(), "FLAG")
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
