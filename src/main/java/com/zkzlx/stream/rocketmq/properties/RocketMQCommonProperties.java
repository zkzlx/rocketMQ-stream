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
	 * Consumers of the same role is required to have exactly same subscriptions and
	 * consumerGroup to correctly achieve load balance. It's required and needs to be
	 * globally unique.
	 * </p>
	 * Producer group conceptually aggregates all producer instances of exactly same role,
	 * which is particularly important when transactional messages are involved.
	 * </p>
	 * <p>
	 * For non-transactional messages, it does not matter as long as it's unique per
	 * process.
	 * </p>
	 * <p>
	 * See <a href="http://rocketmq.apache.org/docs/core-concept/">here</a> for further
	 * discussion.
	 */
	private String group;

	private String namespace;
	private String accessChannel = AccessChannel.LOCAL.name();
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
	private boolean vipChannelEnabled = false;

	private boolean useTLS = TlsSystemConfig.tlsEnable;

	private boolean enableMsgTrace = true;
	private String customizedTraceTopic;

	public String getNameServer() {
		return nameServer;
	}

	public void setNameServer(String nameServer) {
		this.nameServer = nameServer;
	}

	public String getAccessKey() {
		return accessKey;
	}

	public void setAccessKey(String accessKey) {
		this.accessKey = accessKey;
	}

	public String getSecretKey() {
		return secretKey;
	}

	public void setSecretKey(String secretKey) {
		this.secretKey = secretKey;
	}

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public String getNamespace() {
		return namespace;
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	public String getAccessChannel() {
		return accessChannel;
	}

	public void setAccessChannel(String accessChannel) {
		this.accessChannel = accessChannel;
	}

	public int getPollNameServerInterval() {
		return pollNameServerInterval;
	}

	public void setPollNameServerInterval(int pollNameServerInterval) {
		this.pollNameServerInterval = pollNameServerInterval;
	}

	public int getHeartbeatBrokerInterval() {
		return heartbeatBrokerInterval;
	}

	public void setHeartbeatBrokerInterval(int heartbeatBrokerInterval) {
		this.heartbeatBrokerInterval = heartbeatBrokerInterval;
	}

	public int getPersistConsumerOffsetInterval() {
		return persistConsumerOffsetInterval;
	}

	public void setPersistConsumerOffsetInterval(int persistConsumerOffsetInterval) {
		this.persistConsumerOffsetInterval = persistConsumerOffsetInterval;
	}

	public long getPullTimeDelayMillsWhenException() {
		return pullTimeDelayMillsWhenException;
	}

	public void setPullTimeDelayMillsWhenException(long pullTimeDelayMillsWhenException) {
		this.pullTimeDelayMillsWhenException = pullTimeDelayMillsWhenException;
	}

	public boolean isVipChannelEnabled() {
		return vipChannelEnabled;
	}

	public void setVipChannelEnabled(boolean vipChannelEnabled) {
		this.vipChannelEnabled = vipChannelEnabled;
	}

	public boolean isUseTLS() {
		return useTLS;
	}

	public void setUseTLS(boolean useTLS) {
		this.useTLS = useTLS;
	}

	public boolean isEnableMsgTrace() {
		return enableMsgTrace;
	}

	public void setEnableMsgTrace(boolean enableMsgTrace) {
		this.enableMsgTrace = enableMsgTrace;
	}

	public String getCustomizedTraceTopic() {
		return customizedTraceTopic;
	}

	public void setCustomizedTraceTopic(String customizedTraceTopic) {
		this.customizedTraceTopic = customizedTraceTopic;
	}
}
