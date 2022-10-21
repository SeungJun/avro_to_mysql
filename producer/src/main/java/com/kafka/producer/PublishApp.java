package com.kafka.producer;

import com.kafka.core.avro.Dataset;
import com.kafka.producer.manager.PublishClientFactory;
import com.kafka.producer.manager.impl.DefaultPublishFactoryImpl;
import org.apache.kafka.clients.producer.Producer;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.concurrent.ThreadLocalRandom;

//import static com.kafka.core.common.KafkaConstant.*;


public class PublishApp {

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final String brokerList = "http://127.0.0.1:9092";


    public static void main(String[] args) {


        PublishClientFactory publishFactory = new DefaultPublishFactoryImpl(brokerList);
        Producer<String, Dataset> producer = publishFactory.createProducer();

        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        Double randomDouble = ThreadLocalRandom.current().nextDouble();
        Long randomLong = ThreadLocalRandom.current().nextLong();

//        publishFactory.sendEvent();

        try {
            for (int i = 0; i < 23; i++) {
//                final String orderId = "this is key id" + Long.toString(i);
//                Object dataset = new Object(orderId ,randomLong, sdf.format(timestamp), randomDouble);
//                Dataset dataset = new Dataset(orderId , 1L, sdf.format(timestamp), 100.00d );
//                ProducerRecord<String, Object> record = new ProducerRecord<>(TOPIC, orderId , new Object());
//                producer.send(record, new PublishCallback(record));

//                System.out.printf("-> Topic: %s, Partition: %s, Key: %s, Value: %s\n",
//                        record.topic(), record.partition(),  record.key(), record.value());
            }
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            producer.close(); // 프로듀서 종료
        }
    }
}