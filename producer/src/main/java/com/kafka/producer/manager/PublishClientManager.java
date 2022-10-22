package com.kafka.producer.manager;

import com.kafka.core.util.RandomGenerator;
import com.kafka.message.avro.Dataset;
import com.kafka.producer.manager.impl.DefaultPublishFactoryImpl;
import com.kafka.producer.sender.SenderRecordHandler;
import org.apache.kafka.clients.producer.Producer;

import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class PublishClientManager implements AutoCloseable {

	private final ScheduledExecutorService executorService;
	private final String brokerList;
	private final String schemaRegistry;
	private Producer<String, Dataset> producer ;
//	private static final long iterationCount = Long.MAX_VALUE;
	private static final int threadPoolSize = 100;

	public PublishClientManager(PublishClientFactory publishFactory, String brokerList, String schemaRegistry)
	{
		this.executorService = Executors.newScheduledThreadPool(threadPoolSize);
		this.brokerList = brokerList;
		this.producer = publishFactory.createProducer();
		this.schemaRegistry = schemaRegistry;
	}


	public void publish(){

		PublishClientFactory publishFactory = new DefaultPublishFactoryImpl(brokerList, schemaRegistry);

		Producer<String, Dataset> producer = publishFactory.createProducer();

		Stream.iterate(0, i-> i< 1000 , i-> i+1)
//				.map(v-> v%500)
				.forEach(thread -> {

					System.out.println(String.format("-> thread : %s", thread));
					CountDownLatch latch = new CountDownLatch(threadPoolSize);

					LongStream.iterate(1, j-> j< threadPoolSize, j-> j+1)
							.boxed()
							.map(startId -> new SenderRecordHandler(startId,publishFactory
									,  new Dataset(String.valueOf(startId) , Instant.now().getEpochSecond(), RandomGenerator.generateRandomString() ,RandomGenerator.generateRangedNumber()) ,latch))

							.forEach(handler -> executorService.scheduleAtFixedRate(handler, 0 , 1, TimeUnit.MILLISECONDS));


					try {
						latch.await();
					} catch (InterruptedException e) {
						System.out.println("finish threads with interrupt");
						e.printStackTrace();
					}

				});
	}


	@Override
	public void close() {
		if (producer != null) {
			System.out.println("close producer");
			producer.close();
		}

		if (executorService != null) {
			System.out.println("close executor");
			executorService.shutdown();
		}

		try {
			if (!executorService.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
				System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
				executorService.shutdownNow();
			}
		} catch (InterruptedException e) {
			System.out.println("Interrupted during shutdown, exiting uncleanly");
		}
	}
}
