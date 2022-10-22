package com.kafka.producer;

import com.kafka.core.common.KafkaConstant;
import com.kafka.producer.manager.PublishClientFactory;
import com.kafka.producer.manager.PublishClientManager;
import com.kafka.producer.manager.impl.DefaultPublishFactoryImpl;
import org.apache.kafka.clients.producer.Producer;


public class PublishApplication {

    public static void main(String[] args) throws Exception{

        PublishClientFactory publishClientFactory = new DefaultPublishFactoryImpl(KafkaConstant.KAFKA_BROKERS, KafkaConstant.SCHEMA_REGISTRY_URL);
//        Producer<String, ?> producer = publishClientFactory.createProducer();

        PublishClientManager publishClientManager = new PublishClientManager(publishClientFactory, KafkaConstant.KAFKA_BROKERS, KafkaConstant.SCHEMA_REGISTRY_URL);
        publishClientManager.publish();

    }
}