package com.kafka.producer.manager;

import com.kafka.message.avro.Dataset;
import org.apache.kafka.clients.producer.Producer;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public interface PublishClientFactory<T> {

	void sendEvent(String topic, String key, T event);

	Producer<String, T> createProducer();

	static Map<Integer, List<Dataset>> createAvroDateset(){

		Map<Integer, List<Dataset>> avroMap = new LinkedHashMap<>();


		return null;
	}

}
