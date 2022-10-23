package com.kafka.consumer.subscribe;

import com.kafka.consumer.database.DatabaseFactory;
import com.kafka.consumer.model.AvroData;
import com.kafka.message.avro.Dataset;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.kafka.core.common.KafkaConstant.KAFKA_TOPIC;

public class SubscribeHandler implements Runnable{

	private final Consumer consumer ;
	private volatile boolean doneConsuming;
	private DatabaseFactory dbFactory;

	public SubscribeHandler(Consumer consumer,  boolean doneConsume, DatabaseFactory databaseFactory){
		this.consumer = consumer;
		this.doneConsuming = doneConsume;
		this.dbFactory = databaseFactory;
	}

	@Override
	public void run() {

		AvroData data = new AvroData();

		consumer.subscribe(Collections.singleton(KAFKA_TOPIC));

		Map<String, Dataset> avroDataMap = new LinkedHashMap<>();


		while (!doneConsuming) {
			final ConsumerRecords<String, Dataset> records = consumer.poll(Duration.ofMillis(1000));

			for (final ConsumerRecord<String, Dataset> record : records) {

				final String key = record.key();
				final Dataset value = record.value();

				System.out.printf(String.format("---> Topic: %s, consumed Partition: %s, Offset: %d, Key: %s, Value: %s\n",
						record.topic(), record.partition(), record.offset(), key, value));

				avroDataMap.put(key, value);
			}
		}

		data.setDatasetMap(avroDataMap);

		if(!avroDataMap.isEmpty()){
			//insert into db
			dbFactory.subscribeAvro(avroDataMap);
//			MyDatabase.subscribeAvro(linkedMap);
		}
	}
}
