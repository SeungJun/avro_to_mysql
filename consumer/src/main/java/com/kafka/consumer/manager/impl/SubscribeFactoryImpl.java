package com.kafka.consumer.manager.impl;

import com.kafka.consumer.config.SubscribeConfig;
import com.kafka.consumer.manager.SubscribeFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class SubscribeFactoryImpl<T> implements SubscribeFactory {


	@Override
	public Consumer<String, T> createConsumer() {
		return new KafkaConsumer<>(SubscribeConfig.consumerConfig());

	}


}
