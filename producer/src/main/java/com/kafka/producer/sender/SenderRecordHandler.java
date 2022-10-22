package com.kafka.producer.sender;

import com.kafka.message.avro.Dataset;
import com.kafka.producer.manager.PublishClientFactory;
import org.apache.kafka.clients.producer.Producer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static com.kafka.core.common.KafkaConstant.KAFKA_TOPIC;

public class SenderRecordHandler<T> implements Runnable{

	private final PublishClientFactory publishFactory;
	private final CountDownLatch latch;
	private final T dataset;
	private final long publishId;
	private final AtomicInteger counter;


	public SenderRecordHandler(Long publishId, PublishClientFactory publishClientFactory, T dataset, CountDownLatch latch) {
		this.publishFactory = publishClientFactory;
		this.latch = latch;
		this.dataset = dataset;
		this.publishId = publishId;
		this.counter = new AtomicInteger();
	}


	@Override
	public void run() {

		String threadName = Thread.currentThread().getName();
		counter.incrementAndGet();


		Producer<String, Dataset> producer = publishFactory.createProducer();
		publishFactory.sendEvent(KAFKA_TOPIC, String.valueOf(publishId), dataset );
//		publishFactory.sendEvent(KAFKA_TOPIC, publishId, dataset );


//		ProducerRecord<String, Object> record = new ProducerRecord<>(TOPIC, orderId , new Object());
//		producer.send(record, new PublishCallback(record));

		latch.countDown();

	}
}
