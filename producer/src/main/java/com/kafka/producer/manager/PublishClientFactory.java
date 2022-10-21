package com.kafka.producer.manager;

import org.apache.kafka.clients.producer.Producer;

public interface PublishClientFactory<T> {

	void sendEvent(String topic, String key, T event);

	Producer<String, T> createProducer();

}
