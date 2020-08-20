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

package com.zkzlx.stream.rocketmq.contants;

import org.apache.rocketmq.common.message.MessageConst;

/**
 * @author zkzlx
 */
public class RocketMQConst extends MessageConst {
	public static final String USER_SELECTOR_ARGS = "SELECTOR_ARGS";
	public static final String USER_TRANSACTIONAL_ARGS = "TRANSACTIONAL_ARGS";

	public static final String USER_PREFIX = "rocketmq_";
	public static final String USER_KEYS = MessageConst.PROPERTY_KEYS;
	public static final String USER_TAGS = MessageConst.PROPERTY_TAGS;
	public static final String USER_TOPIC = "TOPIC";
	public static final String USER_MESSAGE_ID = "MESSAGE_ID";
	public static final String USER_BORN_TIMESTAMP = "BORN_TIMESTAMP";
	public static final String USER_BORN_HOST = "BORN_HOST";
	public static final String USER_FLAG = "FLAG";
	public static final String USER_QUEUE_ID = "QUEUE_ID";
	public static final String USER_SYS_FLAG = "SYS_FLAG";
	public static final String USER_TRANSACTION_ID = "TRANSACTION_ID";

}
