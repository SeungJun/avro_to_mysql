package com.kafka.consumer;

import com.kafka.consumer.database.DatabaseFactory;
import com.kafka.consumer.database.impl.DatabaseFactoryImpl;
import com.kafka.consumer.manager.SubscribeFactory;
import com.kafka.consumer.manager.impl.SubscribeFactoryImpl;
import com.kafka.consumer.subscribe.SubscribeExecutor;

import java.sql.Connection;


public class SubscriberApplication {

    /**
     * thread sleep time is 2 seconds
     * @param args
     * @throws InterruptedException
     */
    public static void main(final String[] args) throws InterruptedException {

        Connection connection = null;
        SubscribeFactory subscribeFactory = new SubscribeFactoryImpl();
        DatabaseFactory databaseFactory = new DatabaseFactoryImpl(connection);

        SubscribeExecutor subscribeExecutor = new SubscribeExecutor(subscribeFactory, databaseFactory);
        subscribeExecutor.execute();

    }

}
