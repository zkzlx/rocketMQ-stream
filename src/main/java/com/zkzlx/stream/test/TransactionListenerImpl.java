/*
 * Copyright 2013-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zkzlx.stream.test;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * @author <a href="mailto:fangjian0423@gmail.com">Jim</a>
 */
public class TransactionListenerImpl implements TransactionListener {


	/**
	 * When send transactional prepare(half) message succeed, this method will be invoked to execute local transaction.
	 *
	 * @param msg Half(prepare) message
	 * @param arg Custom business parameter
	 * @return Transaction state
	 */
	@Override
	public LocalTransactionState executeLocalTransaction(org.apache.rocketmq.common.message.Message msg, Object arg) {
		Object num = msg.getProperties().get("test");

		if ("1".equals(num)) {
			System.out.println(
					"executer: " + new String(msg.getBody()) + " unknown");
			return LocalTransactionState.UNKNOW;
		}
		else if ("2".equals(num)) {
			System.out.println(
					"executer: " + new String(msg.getBody()) + " rollback");
			return LocalTransactionState.ROLLBACK_MESSAGE;
		}
		System.out.println(
				"executer: " + new String(msg.getBody()) + " commit");
		return LocalTransactionState.COMMIT_MESSAGE;
	}

	/**
	 * When no response to prepare(half) message. broker will send check message to check the transaction status, and this
	 * method will be invoked to get local transaction status.
	 *
	 * @param msg Check message
	 * @return Transaction state
	 */
	@Override
	public LocalTransactionState checkLocalTransaction(MessageExt msg) {
		System.out.println("check: " + new String(msg.getBody()));
		return LocalTransactionState.COMMIT_MESSAGE;
	}
}
