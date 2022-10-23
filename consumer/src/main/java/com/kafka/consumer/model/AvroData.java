package com.kafka.consumer.model;

import com.kafka.message.avro.Dataset;

import java.util.LinkedHashMap;
import java.util.Map;

public class AvroData<T> implements Subscriber<T>{

	public AvroData(){}

	private Map<String, T> datasetMap= new LinkedHashMap<>();


	public Map<String, T> getDatasetMap() {
		return datasetMap;
	}

	public void setDatasetMap(Map<String, T> datasetMap) {
		this.datasetMap = datasetMap;
	}

	@Override
	public void handleMessage(String message) {

	}

	@Override
	public void handleAvroMessage(T dataMessage) {

	}
}
