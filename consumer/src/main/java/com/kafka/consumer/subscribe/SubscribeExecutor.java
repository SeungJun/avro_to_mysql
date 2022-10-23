package com.kafka.consumer.subscribe;

import com.kafka.consumer.database.DatabaseFactory;
import com.kafka.consumer.manager.SubscribeFactory;
import com.kafka.core.common.KafkaConstant;
import org.apache.kafka.clients.consumer.Consumer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SubscribeExecutor implements AutoCloseable{

	private volatile boolean doneConsuming;
	private ExecutorService executor;
	private final Consumer consumer;
	private final DatabaseFactory dbfactory;


	public SubscribeExecutor(SubscribeFactory subscribeFactory, DatabaseFactory databaseFactory){
		this.consumer = subscribeFactory.createConsumer();
		this.dbfactory = databaseFactory;
		this.doneConsuming = false;
		this.executor = Executors.newFixedThreadPool(KafkaConstant.PARTITION_SIZE);
	}


	/**
	 *
	 * @throws InterruptedException
	 */
	public void execute() {
		dbfactory.initConnection();
//		MyDatabase.initConnection();
		startConsuming(KafkaConstant.PARTITION_SIZE, doneConsuming);
//		Thread.sleep(2000);
		stopConsuming();
	}


	/**
	 * execute thread
	 * @param partitionSize
	 * @param doneConsuming
	 */
	public void startConsuming(int partitionSize, boolean doneConsuming) {

		for (int i = 0; i < partitionSize; i++) {
			executor.submit(new SubscribeHandler(consumer, doneConsuming, dbfactory));
		}
	}



	/**
	 * stop subscribe kafka topic
	 * await 1seconds
	 * @throws InterruptedException
	 */
	public void stopConsuming(){
		doneConsuming = true;
		try {
			if (!executor.awaitTermination(3, TimeUnit.SECONDS)) {
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
