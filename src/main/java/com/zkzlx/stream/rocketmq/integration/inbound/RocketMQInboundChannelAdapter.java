package com.zkzlx.stream.rocketmq.integration.inbound;

import com.zkzlx.stream.rocketmq.listener.RocketMQListenerBindingContainer;
import com.zkzlx.stream.rocketmq.properties.RocketMQConsumerProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.integration.context.OrderlyShutdownCapable;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
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

    private RocketMQListenerBindingContainer rocketMQListenerContainer;

    private final ExtendedConsumerProperties<RocketMQConsumerProperties> consumerProperties;

//    private final InstrumentationManager instrumentationManager;

    public RocketMQInboundChannelAdapter(
            RocketMQListenerBindingContainer rocketMQListenerContainer,
            ExtendedConsumerProperties<RocketMQConsumerProperties> consumerProperties
//            ,InstrumentationManager instrumentationManager
            ) {
        this.rocketMQListenerContainer = rocketMQListenerContainer;
        this.consumerProperties = consumerProperties;
//        this.instrumentationManager = instrumentationManager;
    }

    @Override
    protected void onInit() {
        if (consumerProperties == null
                || !consumerProperties.getExtension().getEnabled()) {
            return;
        }
        super.onInit();
        if (this.retryTemplate != null) {
            Assert.state(getErrorChannel() == null,
                    "Cannot have an 'errorChannel' property when a 'RetryTemplate' is "
                            + "provided; use an 'ErrorMessageSendingRecoverer' in the 'recoveryCallback' property to "
                            + "send an error message when retries are exhausted");
        }

        BindingRocketMQListener listener = new BindingRocketMQListener();
        rocketMQListenerContainer.setRocketMQListener(listener);

        if (retryTemplate != null) {
            this.retryTemplate.registerListener(listener);
        }

        try {
            rocketMQListenerContainer.afterPropertiesSet();

        }
        catch (Exception e) {
            log.error("rocketMQListenerContainer init error: " + e.getMessage(), e);
            throw new IllegalArgumentException(
                    "rocketMQListenerContainer init error: " + e.getMessage(), e);
        }

//        instrumentationManager.addHealthInstrumentation(
//                new Instrumentation(rocketMQListenerContainer.getTopic()
//                        + rocketMQListenerContainer.getConsumerGroup()));
    }

    @Override
    protected void doStart() {
        if (consumerProperties == null
                || !consumerProperties.getExtension().getEnabled()) {
            return;
        }
        try {
            rocketMQListenerContainer.start();
//            instrumentationManager
//                    .getHealthInstrumentation(rocketMQListenerContainer.getTopic()
//                            + rocketMQListenerContainer.getConsumerGroup())
//                    .markStartedSuccessfully();
        }
        catch (Exception e) {
//            instrumentationManager
//                    .getHealthInstrumentation(rocketMQListenerContainer.getTopic()
//                            + rocketMQListenerContainer.getConsumerGroup())
//                    .markStartFailed(e);
            log.error("RocketMQTemplate startup failed, Caused by " + e.getMessage());
            throw new MessagingException(MessageBuilder.withPayload(
                    "RocketMQTemplate startup failed, Caused by " + e.getMessage())
                    .build(), e);
        }
    }

    @Override
    protected void doStop() {
        rocketMQListenerContainer.stop();
    }

    public void setRetryTemplate(RetryTemplate retryTemplate) {
        this.retryTemplate = retryTemplate;
    }

    public void setRecoveryCallback(RecoveryCallback<? extends Object> recoveryCallback) {
        this.recoveryCallback = recoveryCallback;
    }

    protected class BindingRocketMQListener
            implements RocketMQListener<Message>, RetryListener {

        @Override
        public void onMessage(Message message) {
            boolean enableRetry = RocketMQInboundChannelAdapter.this.retryTemplate != null;
            if (enableRetry) {
                RocketMQInboundChannelAdapter.this.retryTemplate.execute(context -> {
                    RocketMQInboundChannelAdapter.this.sendMessage(message);
                    return null;
                }, (RecoveryCallback<Object>) RocketMQInboundChannelAdapter.this.recoveryCallback);
            }
            else {
                RocketMQInboundChannelAdapter.this.sendMessage(message);
            }
        }

        @Override
        public <T, E extends Throwable> boolean open(RetryContext context,
                                                     RetryCallback<T, E> callback) {
            return true;
        }

        @Override
        public <T, E extends Throwable> void close(RetryContext context,
                                                   RetryCallback<T, E> callback, Throwable throwable) {
        }

        @Override
        public <T, E extends Throwable> void onError(RetryContext context,
                                                     RetryCallback<T, E> callback, Throwable throwable) {

        }

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
