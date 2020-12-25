package com.zkzlx.stream.rocketmq.extend;

import org.springframework.integration.acks.AcknowledgmentCallback.Status;
import org.springframework.messaging.Message;

/**
 * @author zkz
 */
public interface ErrorAcknowledgeHandler {

    /**
     * Ack state handling, including receive, reject, and retry, when a consumption exception occurs.
     * @param message
     * @return see {@link Status}
     */
    Status handler(Message<?> message);

}
