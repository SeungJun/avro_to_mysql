package com.kafka.consumer.subscribe;

import com.kafka.consumer.model.AvroData;
import com.kafka.db.MyDatabase;
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
	private final String kafkaTopic;
	private volatile boolean doneConsuming;

	public SubscribeHandler(Consumer consumer,  String topic, boolean doneConsume){
		this.consumer = consumer;
		this.kafkaTopic = topic;
		this.doneConsuming = doneConsume;
	}



	@Override
	public void run() {

		AvroData data = new AvroData();

		consumer.subscribe(Collections.singleton(KAFKA_TOPIC));

		Map<String, Dataset> linkedMap = new LinkedHashMap<>();


		while (!doneConsuming) {
			final ConsumerRecords<String, Dataset> records = consumer.poll(Duration.ofMillis(1000));

			for (final ConsumerRecord<String, Dataset> record : records) {

				final String key = record.key();
				final Dataset value = record.value();
				System.out.printf("---> Topic: %s, consumed Partition: %s, Offset: %d, Key: %s, Value: %s\n",
						record.topic(), record.partition(), record.offset(), key, value);

				linkedMap.put(key, value);
			}
		}

		data.setDatasetMap(linkedMap);

		if(!linkedMap.isEmpty()){
			//insert into db
			MyDatabase.subscribeAvro(linkedMap);
		}
	}
}
