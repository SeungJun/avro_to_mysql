package com.kafka.consumer;

import com.kafka.consumer.manager.SubscribeFactory;
import com.kafka.consumer.manager.impl.SubscribeFactoryImpl;
import com.kafka.consumer.subscribe.SubscribeExecutor;
import com.kafka.core.common.KafkaConstant;

import static com.kafka.core.common.KafkaConstant.KAFKA_TOPIC;
import static com.kafka.core.common.KafkaConstant.SCHEMA_REGISTRY_URL;


public class SubscriberApplication {

    /**
     * thread sleep time is 2 seconds
     * @param args
     * @throws InterruptedException
     */
    public static void main(final String[] args) throws InterruptedException {


        SubscribeFactory subscribeFactory = new SubscribeFactoryImpl(KafkaConstant.KAFKA_BROKERS, KAFKA_TOPIC);
        SubscribeExecutor subscribeExecutor = new SubscribeExecutor(subscribeFactory, KafkaConstant.KAFKA_BROKERS,SCHEMA_REGISTRY_URL,  KAFKA_TOPIC);
        subscribeExecutor.execute();

    }

}
