package com.kafka.producer;

import com.kafka.producer.manager.PublishClientFactory;
import com.kafka.producer.manager.PublishClientManager;
import com.kafka.producer.manager.impl.DefaultPublishFactoryImpl;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.concurrent.ThreadLocalRandom;



public class PublishApp {

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final String brokerList = "http://127.0.0.1:9092";


    public static void main(String[] args) {

        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        Double randomDouble = ThreadLocalRandom.current().nextDouble();
        Long randomLong = ThreadLocalRandom.current().nextLong();


        PublishClientFactory publishClientFactory = new DefaultPublishFactoryImpl(brokerList);

        PublishClientManager publishClientManager = new PublishClientManager(publishClientFactory, brokerList);
        publishClientManager.publish();

        try {

/*            Stream.iterate(0, i -> i < publishCount, i -> i + 1)
                    .map(v -> v % 5000)
                    .forEach(thread -> {

                        CountDownLatch latch = new CountDownLatch(workerThreadCount);

                        try {
                            latch.await();
                        } catch (InterruptedException e) {
                            System.out.println("finish threads with interrupt");
                            e.printStackTrace();
                        }


                    });*/

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
//            producer.close(); // 프로듀서 종료
        }
    }
}