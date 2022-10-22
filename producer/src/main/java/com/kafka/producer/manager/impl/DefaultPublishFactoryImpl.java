package com.kafka.producer.manager.impl;

import com.kafka.producer.config.PublishConfig;
import com.kafka.producer.manager.PublishClientFactory;
import com.kafka.producer.sender.SenderCallback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class DefaultPublishFactoryImpl<T> implements PublishClientFactory<T> {

	private final String brokerList ;

	public DefaultPublishFactoryImpl(String brokers){
		this.brokerList = brokers;
	}

	@Override
	public void sendEvent(String topic, String publishKey, T event)
	{
		ProducerRecord<String, T> record = new ProducerRecord<>(topic, publishKey , event);
		createProducer().send(record, new SenderCallback(record));
	}

	@Override
	public Producer<String, T> createProducer() {

		return new KafkaProducer<>(PublishConfig.defaultProducerConfig(brokerList));
	}
}
