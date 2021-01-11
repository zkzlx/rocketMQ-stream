package com.zkzlx.stream.test;

import org.springframework.cloud.stream.annotation.Output;
import org.springframework.messaging.MessageChannel;

public interface ISource {
    MessageChannel output1();

    MessageChannel output2();

    MessageChannel output3();
}