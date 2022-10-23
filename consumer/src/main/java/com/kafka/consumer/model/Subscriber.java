package com.kafka.consumer.model;

import com.kafka.message.avro.Dataset;

public interface Subscriber<T> {

	void handleMessage(String message);

	void handleAvroMessage(T dataMessage);
}
