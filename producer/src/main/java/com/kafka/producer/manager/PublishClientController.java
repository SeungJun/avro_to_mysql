package com.kafka.producer.manager;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class PublishClientController implements AutoCloseable {

	private final ScheduledExecutorService executorService;

	private final long publishCount ;

	public PublishClientController(int threadCount, long publishCount) {
		this.executorService = Executors.newScheduledThreadPool(threadCount);;
		this.publishCount = publishCount;
	}


	public void publish(){


		LongStream.iterate(0, i-> i<Long.MAX_VALUE , i-> i+1)
				.map(v-> v%5000)
				.forEach(thread -> {

					Stream.iterate(1, j-> j<50000, j-> j+1)
							.limit()


				});
	}



	@Override
	public void close() throws Exception {

	}
}
