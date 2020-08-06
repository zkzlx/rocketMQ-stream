package com.zkzlx.stream.rocketmq.properties;

import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;

/**
 * @author zkz
 */
public class RocketMQCommonProperties {

    private String nameServer;

    /**
     * The property of "access-key".
     */
    private String accessKey;

    /**
     * The property of "secret-key".
     */
    private String secretKey;
    /**
     * Consumers of the same role is required to have exactly same subscriptions and consumerGroup to correctly achieve
     * load balance. It's required and needs to be globally unique.
     * </p>
     * Producer group conceptually aggregates all producer instances of exactly same role, which is particularly
     * important when transactional messages are involved. </p>
     * <p>
     * For non-transactional messages, it does not matter as long as it's unique per process. </p>
     * <p>
     * See <a href="http://rocketmq.apache.org/docs/core-concept/">here</a> for further discussion.
     */
    private String group;


    private String namespace;
    private AccessChannel accessChannel = AccessChannel.LOCAL;
    /**
     * Pulling topic information interval from the named server
     */
    private int pollNameServerInterval = 1000 * 30;
    /**
     * Heartbeat interval in microseconds with message broker
     */
    private int heartbeatBrokerInterval = 1000 * 30;
    /**
     * Offset persistent interval for consumer
     */
    private int persistConsumerOffsetInterval = 1000 * 5;
    private long pullTimeDelayMillsWhenException = 1000;
    private boolean unitMode = false;
    private String unitName;
    private boolean vipChannelEnabled = false;

    private boolean useTLS = TlsSystemConfig.tlsEnable;
}
