package com.kafka.producer;

import com.kafka.Dataset;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import static com.kafka.common.Constant.TOPIC;

public class MyProducer {

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://127.0.0.1:9092");
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        Producer<String, Dataset> producer = new KafkaProducer<>(props);

        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        Double randomDouble = ThreadLocalRandom.current().nextDouble();
        Long randomLong = ThreadLocalRandom.current().nextLong();

        try {
            for (int i = 0; i < 3; i++) {
                final String orderId = "this is key id" + Long.toString(i);
                Dataset dataset = new Dataset(orderId ,randomLong, sdf.format(timestamp), randomDouble);
//                Dataset dataset = new Dataset(orderId , 1L, sdf.format(timestamp), 100.00d );
                ProducerRecord<String, Dataset> record = new ProducerRecord<>(TOPIC, orderId , dataset);
                producer.send(record, new MyCallback(record));

                System.out.printf("-> Topic: %s, Partition: %s, Key: %s, Value: %s\n",
                        record.topic(), record.partition(),  record.key(), record.value());
            }
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            producer.close(); // 프로듀서 종료
        }
    }
}