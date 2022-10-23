package com.kafka.consumer.database;

import com.kafka.message.avro.Dataset;

import java.util.LinkedHashMap;
import java.util.List;

public interface DatabaseFactory{

	void initConnection();

	void subscribeAvro(LinkedHashMap<String, Dataset> avroDatas);

	void insertData(List<Dataset> data);
}
