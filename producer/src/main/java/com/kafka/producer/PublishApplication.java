package com.kafka.producer;

import com.kafka.core.common.KafkaConstant;
import com.kafka.producer.manager.PublishClientFactory;
import com.kafka.producer.thread.PublishClientExecutor;
import com.kafka.producer.manager.impl.DefaultPublishFactoryImpl;


public class PublishApplication {

    public static void main(String[] args) throws Exception{

        PublishClientFactory publishClientFactory
                = new DefaultPublishFactoryImpl(KafkaConstant.KAFKA_BROKERS, KafkaConstant.SCHEMA_REGISTRY_URL);

        PublishClientExecutor publishClientManager
                = new PublishClientExecutor(publishClientFactory, KafkaConstant.KAFKA_BROKERS, KafkaConstant.SCHEMA_REGISTRY_URL);

        publishClientManager.execute();

    }
}