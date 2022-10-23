package com.kafka.producer.thread;

import com.kafka.core.common.KafkaConstant;
import com.kafka.core.util.RandomGenerator;
import com.kafka.message.avro.Dataset;
import com.kafka.producer.manager.PublishClientFactory;
import com.kafka.producer.manager.impl.DefaultPublishFactoryImpl;
import org.apache.kafka.clients.producer.Producer;

import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class PublishClientExecutor implements AutoCloseable {

	private final ScheduledExecutorService executor;
	private final String brokerList;
	private final String schemaRegistry;
	private Producer<String, Dataset> producer ;
	private static final int threadPoolSize = KafkaConstant.THREAD_POOL_SIZE;

	public PublishClientExecutor(PublishClientFactory publishFactory, String brokerList, String schemaRegistry)
	{
		this.executor = Executors.newScheduledThreadPool(threadPoolSize);
		this.brokerList = brokerList;
		this.producer = publishFactory.createProducer();
		this.schemaRegistry = schemaRegistry;
	}


	public void execute(){

		PublishClientFactory publishFactory = new DefaultPublishFactoryImpl(brokerList, schemaRegistry);

//		Producer<String, Dataset> producer = publishFactory.createProducer();

		Stream.iterate(0, i-> i< 1000 , i-> i+1)
				.forEach(thread -> {

					System.out.println(String.format("-> thread : %s", thread));
					CountDownLatch latch = new CountDownLatch(threadPoolSize);

					LongStream.iterate(1, j-> j< threadPoolSize, j-> j+1)
							.boxed()
							.map(startId -> new SenderRecordHandler(startId,publishFactory
									,  new Dataset(String.valueOf(startId) , Instant.now().getEpochSecond(), RandomGenerator.generateRandomString() ,RandomGenerator.generateRangedNumber()) ,latch))

							.forEach(handler -> executor.scheduleAtFixedRate(handler, 0 , 1, TimeUnit.MILLISECONDS));

					try {
						latch.await();
					} catch (InterruptedException e) {
						System.out.println("finish threads with interrupt :" + e);
//						e.printStackTrace();
					}
				});
	}


	@Override
	public void close() {
		if (producer != null) {
			System.out.println("close producer");
			producer.close();
		}

		if (executor != null) {
			System.out.println("close executor");
			executor.shutdown();
		}

		try {
			if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
				System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
				executor.shutdownNow();
			}
		} catch (InterruptedException e) {
			System.out.println("Interrupted during shutdown, exiting uncleanly");
		}
	}
}
