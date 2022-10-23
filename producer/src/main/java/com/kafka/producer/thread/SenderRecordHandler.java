package com.kafka.producer.thread;

import com.kafka.producer.manager.PublishClientFactory;

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

		System.out.println(String.format("=> publish count : %s" ,threadName));

		publishFactory.sendEvent(KAFKA_TOPIC, String.valueOf(publishId), dataset );
		latch.countDown();
	}
}
