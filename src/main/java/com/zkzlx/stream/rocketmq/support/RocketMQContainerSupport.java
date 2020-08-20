package com.zkzlx.stream.rocketmq.support;

import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.springframework.messaging.MessageChannel;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Gets the beans configured in the configuration file
 *
 * @author junboXiang
 */
public class RocketMQContainerSupport {

    private static final Map<String, AllocateMessageQueueStrategy> allocateMessageQueueStrategyCache = new ConcurrentHashMap<>();
    private static final Map<String, MessageListener> messageListenerCache = new ConcurrentHashMap<>();
    private static final Map<String, OffsetStore> offsetStoreCache = new ConcurrentHashMap<>();
    private static final Map<String, MessageChannel> messageChannelCache = new ConcurrentHashMap<>();

    public static AllocateMessageQueueStrategy getAllocateMessageQueueStrategy(String key) {
        return allocateMessageQueueStrategyCache.get(key);
    }

    public static MessageListener getMessageListener(String key) {
        return messageListenerCache.get(key);
    }

    public static OffsetStore getOffsetStore(String key) {
        return offsetStoreCache.get(key);
    }

    public static MessageChannel getMessageChannel(String key) {
        return messageChannelCache.get(key);
    }



    public static void putAllocateMessageQueueStrategyCache(String beanName, AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        allocateMessageQueueStrategyCache.put(beanName, allocateMessageQueueStrategy);
    }

    public static void putMessageListenerCache(String beanName, MessageListener messageListener) {
        messageListenerCache.put(beanName, messageListener);
    }

    public static void putOffsetStoreCache(String beanName, OffsetStore offsetStore) {
        offsetStoreCache.put(beanName, offsetStore);
    }

    public static void putMessageChannelCache(String beanName, MessageChannel messageChannel) {
        messageChannelCache.put(beanName, messageChannel);
    }
}
