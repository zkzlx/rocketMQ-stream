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

package com.zkzlx.stream;


import java.io.IOException;

import com.zkzlx.stream.test.Foo;
import com.zkzlx.stream.test.ISource;
import com.zkzlx.stream.test.SenderService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.FilterType;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.handler.annotation.Payload;

/**
 * @author <a href="mailto:fangjian0423@gmail.com">Jim</a>
 */
@SpringBootApplication
@EnableBinding({ RocketMQForAliYunApplication.MySource.class, RocketMQForAliYunApplication.MySink.class })
@ComponentScan(value = "com.zkzlx.stream",excludeFilters = @Filter(type = FilterType.ASSIGNABLE_TYPE
		,classes = {RocketMQConsumerApplication.class,RocketMQProduceApplication.class}))
public class RocketMQForAliYunApplication {

	/*
	 * 需要在VM参数里加入下方配置
	 * -Dspring.profiles.active=aliyun
	 */

	public static void main(String[] args) throws IOException {
		SpringApplication.run(RocketMQForAliYunApplication.class);
	}

	@Bean
	public CustomRunner customRunner() {
		return new CustomRunner("output1");
	}

	@Bean
	public SenderService senderService(){
		return new SenderService();
	}

	public interface MySource extends ISource {

		@Override
		@Output("output1")
		MessageChannel output1();

	}

	public static class CustomRunner implements CommandLineRunner {

		private final String bindingName;

		public CustomRunner(String bindingName) {
			this.bindingName = bindingName;
		}

		@Autowired
		private SenderService senderService;

		@Override
		public void run(String... args) throws Exception {
			int count = 20;
			for (int index = 1; index <= count; index++) {
				String msgContent = "msg-" + index;
				if (index % 3 == 0) {
					senderService.send(msgContent);
				}
				else if (index % 3 == 1) {
					senderService.sendWithTags(msgContent, "tagStr");
				}
				else {
					senderService.sendObject(new Foo(index, "foo"), "tagObj");
				}
			}

		}

	}


	@Bean
	public ReceiveService receiveService(){
		return new ReceiveService();
	}

	public interface MySink {

		@Input("input1")
		SubscribableChannel input1();
		@Input("input3")
		SubscribableChannel input3();
	}

	public static class ReceiveService {

		@StreamListener("input1")
		public void receiveInput1(String receiveMsg) {
			System.out.println("input1 receive: " + receiveMsg);
		}


		@StreamListener("input3")
		public void receiveInput3(@Payload Foo foo) {
			System.out.println("input3 receive: " + foo);
		}

	}

}
