package com.kafka.consumer.database.impl;

import com.kafka.consumer.database.DatabaseFactory;
import com.kafka.message.avro.Dataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.LinkedHashMap;
import java.util.List;

public class DatabaseFactoryImpl implements DatabaseFactory {

	private static final Logger LOG = LoggerFactory.getLogger(DatabaseFactoryImpl.class);

	private static Connection connection;
	private static String insertStatement = "insert into MyTest(consumer_key, message, TimeDate, NumberIdentity) values(?,?,?,?); ";

	@Override
	public void initConnection() {

	}

	@Override
	public void subscribeAvro(LinkedHashMap<String, Dataset> avroDatas) {

	}

	@Override
	public void insertData(List<Dataset> data) {

	}
}
