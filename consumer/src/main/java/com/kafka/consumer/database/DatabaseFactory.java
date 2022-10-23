package com.kafka.consumer.database;

import com.kafka.message.avro.Dataset;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public interface DatabaseFactory<T>{

	void initConnection();

	List<Dataset> subscribeAvro(Map<String, Dataset> avroDatas);

	void insertData(List<Dataset> data);
}
