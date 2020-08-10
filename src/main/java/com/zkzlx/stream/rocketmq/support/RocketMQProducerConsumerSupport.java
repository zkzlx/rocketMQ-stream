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

import java.lang.reflect.Field;

import com.zkzlx.stream.rocketmq.properties.RocketMQBinderConfigurationProperties;
import com.zkzlx.stream.rocketmq.properties.RocketMQConsumerProperties;
import com.zkzlx.stream.rocketmq.properties.RocketMQProducerProperties;
import com.zkzlx.stream.rocketmq.properties.RocketMQProducerProperties.ProducerType;
import com.zkzlx.stream.rocketmq.utils.RocketMQUtils;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.trace.AsyncTraceDispatcher;
import org.apache.rocketmq.client.trace.TraceDispatcher;
import org.apache.rocketmq.client.trace.hook.SendMessageTraceHookImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.remoting.RPCHook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Extended function related to producer and consumer. eg:initial
 * @author zkzlx
 */
public class RocketMQProducerConsumerSupport {
    private final static Logger log = LoggerFactory.getLogger(RocketMQProducerConsumerSupport.class);


    /**
     *  init for the producer,including convert producer params.
     * @param producerDestination
     * @param binderConfigurationProperties
     * @param extendedProperties
     * @return
     */
    public static DefaultMQProducer initRocketMQProducer(ProducerDestination producerDestination
                                                  ,RocketMQBinderConfigurationProperties binderConfigurationProperties
            , ExtendedProducerProperties<RocketMQProducerProperties> extendedProperties){
        RocketMQProducerProperties producerProperties =RocketMQUtils.mergeRocketMQProducerProperties(
                binderConfigurationProperties,extendedProperties.getExtension());
        Assert.notNull(producerProperties.getGroup(), "Property 'consumerGroup' is required");
        Assert.notNull(producerProperties.getNameServer(), "Property 'nameServer' is required");

        RPCHook rpcHook=null;
        String ak = producerProperties.getAccessKey();
        String sk = producerProperties.getSecretKey();
        if (!StringUtils.isEmpty(ak) && !StringUtils.isEmpty(sk)) {
            rpcHook = new AclClientRPCHook(new SessionCredentials(ak, sk));
            producerProperties.setVipChannelEnabled(false);
        }
        DefaultMQProducer producer ;
        if(ProducerType.Trans.equalsName(producerProperties.getProducerType())){
            producer = new TransactionMQProducer(producerProperties.getNamespace()
                    ,producerProperties.getGroup(),rpcHook);
        }else{
            producer = new DefaultMQProducer(producerProperties.getNamespace()
                    ,producerProperties.getGroup(),rpcHook);
        }
        if (producerProperties.isEnableMsgTrace()) {
            try {
                AsyncTraceDispatcher dispatcher = new AsyncTraceDispatcher(
                        producerProperties.getGroup(), TraceDispatcher.Type.PRODUCE, producerProperties.getCustomizedTraceTopic(), rpcHook);
                dispatcher.setHostProducer(producer.getDefaultMQProducerImpl());
                Field field = DefaultMQProducer.class.getDeclaredField("traceDispatcher");
                field.setAccessible(true);
                field.set(producer, dispatcher);
                producer.getDefaultMQProducerImpl().registerSendMessageHook(
                        new SendMessageTraceHookImpl(dispatcher));
            } catch (Throwable e) {
                log.error("system mqtrace hook init failed ,maybe can't send msg trace data");
            }
        }

        producer.setVipChannelEnabled(producerProperties.isVipChannelEnabled());
        producer.setInstanceName(RocketMQUtils.getInstanceName(rpcHook,producerDestination.getName() + "|" + UtilAll.getPid()));
        producer.setNamesrvAddr(producerProperties.getNameServer());
        producer.setSendMsgTimeout(producerProperties.getSendMsgTimeout());
        producer.setRetryTimesWhenSendFailed(producerProperties.getRetryTimesWhenSendFailed());
        producer.setRetryTimesWhenSendAsyncFailed(producerProperties.getRetryTimesWhenSendAsyncFailed());
        producer.setCompressMsgBodyOverHowmuch(producerProperties.getCompressMsgBodyThreshold());
        producer.setRetryAnotherBrokerWhenNotStoreOK(producerProperties.isRetryAnotherBroker());
        producer.setMaxMessageSize(producerProperties.getMaxMessageSize());
        return producer;
    }


    /**
     *
     * @param destination
     * @param binderConfigurationProperties
     * @param extendedProperties
     * @return
     */
    public static DefaultMQPushConsumer initRocketMQPushConsumer(ProducerDestination destination
            ,RocketMQBinderConfigurationProperties binderConfigurationProperties
            , ExtendedProducerProperties<RocketMQConsumerProperties> extendedProperties){
        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer();
        return defaultMQPushConsumer;
    }


}