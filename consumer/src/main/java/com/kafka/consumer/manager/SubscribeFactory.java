package com.kafka.consumer.manager;

import org.apache.kafka.clients.consumer.Consumer;

public interface SubscribeFactory<T> {

	Consumer<String, T> createConsumer();

}
