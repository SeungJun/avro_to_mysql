package com.kafka.producer;

import com.kafka.Dataset;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerAsync {

    private static final String TOPIC = "testTopic";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

//        Producer<String, MyRecord> producer = new KafkaProducer<>(props);
        Producer<String, Dataset> producer = new KafkaProducer<>(props);
        try {

            for (int i = 0; i < 4; i++) {
//                ProducerRecord<String, String> record = new ProducerRecord<>("testTopic", "test - " + i);
                final String orderId = "id" + Long.toString(i);
                Dataset dataset = new Dataset(orderId , 1L, orderId, 100.00d );
                ProducerRecord<String, Dataset> record = new ProducerRecord<>(TOPIC, orderId , dataset);
                producer.send(record, new ProducerCallback(record));

                System.out.printf("Successfully produced 10 messages to a topic called %s%n", TOPIC);
            }
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            producer.close(); // 프로듀서 종료
        }
    }
}