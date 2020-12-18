package com.zkzlx.stream.rocketmq.integration.outbound;

import com.zkzlx.stream.rocketmq.properties.RocketMQProducerProperties;
import com.zkzlx.stream.rocketmq.properties.RocketMQProducerProperties.ProducerType;
import com.zkzlx.stream.rocketmq.utils.RocketMQUtils;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.remoting.RPCHook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Extended function related to producer . eg:initial
 *
 * @author zkzlx
 */
public final class RocketMQProduceProcessor {

    private final static Logger log = LoggerFactory.getLogger(RocketMQProduceProcessor.class);


    /**
     *  init for the producer,including convert producer params.
     * @return
     */
    public static DefaultMQProducer initRocketMQProducer(String topic
            , RocketMQProducerProperties producerProperties){

        if(StringUtils.isEmpty(producerProperties.getGroup())){
            producerProperties.setGroup(MixAll.DEFAULT_PRODUCER_GROUP);
        }
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
//            if (producerProperties.getEnableMsgTrace()) {
//                try {
//                    AsyncTraceDispatcher dispatcher = new AsyncTraceDispatcher(
//                            producerProperties.getGroup(), TraceDispatcher.Type.PRODUCE, producerProperties.getCustomizedTraceTopic(), rpcHook);
//                    dispatcher.setHostProducer(producer.getDefaultMQProducerImpl());
//                    Field field = DefaultMQProducer.class.getDeclaredField("traceDispatcher");
//                    field.setAccessible(true);
//                    field.set(producer, dispatcher);
//                    producer.getDefaultMQProducerImpl().registerSendMessageHook(
//                            new SendMessageTraceHookImpl(dispatcher));
//                } catch (Throwable e) {
//                    log.error("system mqtrace hook init failed ,maybe can't send msg trace data");
//                }
//            }
        }else{
            producer = new DefaultMQProducer(producerProperties.getNamespace()
                    ,producerProperties.getGroup(),rpcHook,producerProperties.getEnableMsgTrace(),producerProperties.getCustomizedTraceTopic());
        }

        producer.setVipChannelEnabled(producerProperties.getVipChannelEnabled());
        producer.setInstanceName(RocketMQUtils.getInstanceName(rpcHook,topic + "|" + UtilAll.getPid()));
        producer.setNamesrvAddr(producerProperties.getNameServer());
        producer.setSendMsgTimeout(producerProperties.getSendMsgTimeout());
        producer.setRetryTimesWhenSendFailed(producerProperties.getRetryTimesWhenSendFailed());
        producer.setRetryTimesWhenSendAsyncFailed(producerProperties.getRetryTimesWhenSendAsyncFailed());
        producer.setCompressMsgBodyOverHowmuch(producerProperties.getCompressMsgBodyThreshold());
        producer.setRetryAnotherBrokerWhenNotStoreOK(producerProperties.getRetryAnotherBroker());
        producer.setMaxMessageSize(producerProperties.getMaxMessageSize());
        return producer;
    }


}
