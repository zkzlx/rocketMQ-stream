package com.zkzlx.stream.rocketmq.integration.inbound.pull;

import org.springframework.integration.acks.AcknowledgmentCallback.Status;
import org.springframework.messaging.Message;

import com.zkzlx.stream.rocketmq.extend.ErrorAcknowledgeHandler;

/**
 * By default, if consumption fails, the corresponding MessageQueue will always be retried,
 * that is, the consumption of other messages in the MessageQueue will be blocked.
 *
 * @author zkzlx
 */
public class DefaultErrorAcknowledgeHandler implements ErrorAcknowledgeHandler {
    /**
     * Ack state handling, including receive, reject, and retry, when a consumption exception occurs.
     *
     * @param message
     * @return see {@link Status}
     */
    @Override
    public Status handler(Message<?> message) {
        return Status.REQUEUE;
    }
}
