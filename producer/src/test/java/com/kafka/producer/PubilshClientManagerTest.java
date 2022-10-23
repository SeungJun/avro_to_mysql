package com.kafka.producer;

import com.kafka.core.common.KafkaConstant;
import com.kafka.producer.manager.PublishClientFactory;
import com.kafka.producer.thread.PublishClientExecutor;
import com.kafka.producer.manager.impl.DefaultPublishFactoryImpl;
import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;


@ExtendWith(MockitoExtension.class)
public class PubilshClientManagerTest {

	private final PublishClientExecutor publishManager;

	private final Producer<String, ?>  producer;

	private final PublishClientFactory publishClientFactory;

	public PubilshClientManagerTest() {
		this.publishClientFactory = new DefaultPublishFactoryImpl(KafkaConstant.KAFKA_BROKERS, KafkaConstant.SCHEMA_REGISTRY_URL);
		this.producer = publishClientFactory.createProducer();
		this.publishManager = new PublishClientExecutor(publishClientFactory, KafkaConstant.KAFKA_BROKERS, KafkaConstant.SCHEMA_REGISTRY_URL);
//		this.publishManager = publishManager;
//		this.publishClientFactory = factory;
	}

	@BeforeEach
	public void setup(){


//		Producer<String, ?> producer = publishClientFactory.createProducer();



//		new PublishClientManager();
	}


	@Test
	public void publishTest(){
		publishManager.execute();
	}
}
