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
	private Producer<String, Dataset> producer ;
	private static final long iterationCount = Long.MAX_VALUE;
	private static final int workerThreadCount = 10;
	private static final int threadCount = 10;

	public PublishClientManager(PublishClientFactory publishFactory, String brokerList)
	{
		this.executorService = Executors.newScheduledThreadPool(threadCount);
		this.brokerList = brokerList;
		this.producer = publishFactory.createProducer();
	}


	public void publish(){

		PublishClientFactory publishFactory = new DefaultPublishFactoryImpl(brokerList);

//		Dataset dataset = new Dataset(orderId , 1L, LocalDateTime.now()., 100.00d );
//		Dataset example = new Dataset(String.valueOf(startId) , Instant.now().getEpochSecond(), RandomGenerator.generateRandomString() ,RandomGenerator.generateRangedNumber()) ,latch);

		Producer<String, Dataset> producer = publishFactory.createProducer();

		Stream.iterate(0, i-> i< iterationCount , i-> i+1)
				.map(v-> v%5000)
				.forEach(thread -> {

					CountDownLatch latch = new CountDownLatch(workerThreadCount);

					LongStream.iterate(1, j-> j< workerThreadCount, j-> j+5000)
							.boxed()
							.map(startId -> new SenderRecordHandler(startId,publishFactory
									,  new Dataset(String.valueOf(startId) , Instant.now().getEpochSecond(), RandomGenerator.generateRandomString() ,RandomGenerator.generateRangedNumber()) ,latch))
							.forEach(handler -> executorService.scheduleAtFixedRate(handler, 0 , 100, TimeUnit.MILLISECONDS));


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
