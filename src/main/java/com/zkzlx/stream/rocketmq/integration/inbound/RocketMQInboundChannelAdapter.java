package com.zkzlx.stream.rocketmq.integration.inbound;

import com.zkzlx.stream.rocketmq.properties.RocketMQConsumerProperties;
import com.zkzlx.stream.rocketmq.utils.RocketMQUtils;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.integration.context.OrderlyShutdownCapable;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.MessagingException;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;

/**
 * TODO Describe what it does
 *
 * @author zkzlx
 */
public class RocketMQInboundChannelAdapter extends MessageProducerSupport implements
        OrderlyShutdownCapable {

    private static final Logger log = LoggerFactory
            .getLogger(RocketMQInboundChannelAdapter.class);

    private RetryTemplate retryTemplate;
    private RecoveryCallback<? extends Object> recoveryCallback;
    private DefaultMQPushConsumer defaultMQPushConsumer;
    private volatile boolean isRunning = false;


    private String topic ;
    private RocketMQConsumerProperties consumerProperties;
    public RocketMQInboundChannelAdapter(String topic,RocketMQConsumerProperties consumerProperties) {
        this.topic = topic;
        this.consumerProperties =consumerProperties;
    }

    @Override
    protected void onInit() {
        if (consumerProperties == null || !consumerProperties.getEnabled()) {
            return;
        }
        super.onInit();
        if (this.retryTemplate != null) {
            Assert.state(getErrorChannel() == null,
                    "Cannot have an 'errorChannel' property when a 'RetryTemplate' is "
                            + "provided; use an 'ErrorMessageSendingRecoverer' in the 'recoveryCallback' property to "
                            + "send an error message when retries are exhausted");
        }

        try{
            defaultMQPushConsumer = RocketMQConsumerProcessor.initPushConsumer(consumerProperties);
        }catch (Exception e){
            log.error("DefaultMQPushConsumer init failed, Caused by " + e.getMessage());
            throw new MessagingException(MessageBuilder.withPayload(
                    "DefaultMQPushConsumer init failed, Caused by " + e.getMessage())
                    .build(), e);
        }
//        instrumentationManager.addHealthInstrumentation(
//                new Instrumentation(rocketMQListenerContainer.getTopic()
//                        + rocketMQListenerContainer.getConsumerGroup()));
    }

    @Override
    protected void doStart() {
        if (consumerProperties == null || !consumerProperties.getEnabled()) {
            return;
        }
        try {
            defaultMQPushConsumer.subscribe(topic,RocketMQUtils.getMessageSelector(consumerProperties.getSubscription()));
            defaultMQPushConsumer.start();
//            instrumentationManager
//                    .getHealthInstrumentation(rocketMQListenerContainer.getTopic()
//                            + rocketMQListenerContainer.getConsumerGroup())
//                    .markStartedSuccessfully();
            isRunning = true;
        }
        catch (Exception e) {
//            instrumentationManager
//                    .getHealthInstrumentation(rocketMQListenerContainer.getTopic()
//                            + rocketMQListenerContainer.getConsumerGroup())
//                    .markStartFailed(e);
            log.error("DefaultMQPushConsumer init failed, Caused by " + e.getMessage());
            throw new MessagingException(MessageBuilder.withPayload(
                    "DefaultMQPushConsumer init failed, Caused by " + e.getMessage())
                    .build(), e);
        }
    }

    @Override
    protected void doStop() {
        if(isRunning && defaultMQPushConsumer != null) {
            defaultMQPushConsumer.shutdown();
        }
    }

    public void setRetryTemplate(RetryTemplate retryTemplate) {
        this.retryTemplate = retryTemplate;
    }

    public void setRecoveryCallback(RecoveryCallback<? extends Object> recoveryCallback) {
        this.recoveryCallback = recoveryCallback;
    }

    @Override
    public int beforeShutdown() {
        this.stop();
        return 0;
    }

    @Override
    public int afterShutdown() {
        return 0;
    }

}
