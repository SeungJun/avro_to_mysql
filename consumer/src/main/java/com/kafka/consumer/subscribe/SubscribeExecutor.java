package com.kafka.consumer.subscribe;

import com.kafka.consumer.manager.SubscribeFactory;
import com.kafka.db.MyDatabase;
import org.apache.kafka.clients.consumer.Consumer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SubscribeExecutor implements AutoCloseable{

	private volatile boolean doneConsuming = false;
	private int partitionSize = 2;
	private ExecutorService executor;
	private final String brokerList;
	private final String schemeRegistry;
	private final Consumer consumer;
	private final String kafkaTopic;


	public SubscribeExecutor(SubscribeFactory subscribeFactory, String brokerList, String schemeRegistry, String topic){
		this.brokerList =brokerList;
		this.schemeRegistry = schemeRegistry;
		this.consumer = subscribeFactory.createConsumer();
		this.kafkaTopic = topic;
	}


	public void execute() throws InterruptedException {
		MyDatabase.initConnection();
		startConsuming(partitionSize, doneConsuming);
//		Thread.sleep(2000);
		stopConsuming();
	}


	/**
	 * this method get db connection in this timing
	 */
	public void startConsuming(int partitionSize, boolean doneConsuming) {
		executor = Executors.newFixedThreadPool(partitionSize);
		for (int i = 0; i < partitionSize; i++) {
			executor.submit(new SubscribeHandler(consumer, kafkaTopic, doneConsuming));
		}
	}



	/**
	 * await 1seconds
	 * @throws InterruptedException
	 */
	public void stopConsuming() throws InterruptedException {
		doneConsuming = true;
		try {
			if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
				System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
				executor.shutdownNow();
			}
		} catch (InterruptedException e) {
			System.out.println("Interrupted during shutdown, exiting uncleanly");
		}

	}


	@Override
	public void close() throws Exception {

		if (consumer != null) {
			consumer.close();
			System.out.println("consumer shutdown successfully");
		}

		if(executor != null){
			executor.shutdown();
			System.out.println("executor shutdown successfully");
		}
	}
}
