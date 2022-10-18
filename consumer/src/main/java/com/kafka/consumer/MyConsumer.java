package com.kafka.consumer;

import com.kafka.Dataset;
import com.kafka.db.MyDatabase;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.kafka.common.Constant.TOPIC;


public class MyConsumer {

    private volatile boolean doneConsuming = false;
    private int numberPartitions;
    private ExecutorService executorService= Executors.newCachedThreadPool();


    public MyConsumer(int numberPartitions) {
        this.numberPartitions = numberPartitions;
    }


    /**
     * this method get db connection in this timing
     */
    public void startConsuming() {
        executorService = Executors.newFixedThreadPool(numberPartitions);
        Properties properties = getConsumerProps();

        MyDatabase.initConnection();
        for (int i = 0; i < numberPartitions; i++) {
            Runnable consumerThread = getConsumerThread(properties);
            executorService.submit(consumerThread);
        }
    }


    private Properties getConsumerProps() {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:9092");
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        return props;
    }

    /**
     * this method do process db insert
     * @param properties
     * @return
     */
    private Runnable getConsumerThread(Properties properties) {
        LinkedHashMap<String, Dataset> linkedMap = new LinkedHashMap<>();
        return () -> {
            org.apache.kafka.clients.consumer.Consumer<String, Dataset> consumer = null;

            try {
                consumer = new KafkaConsumer<>(properties);
                consumer.subscribe(Collections.singletonList(TOPIC));
                while (!doneConsuming) {
                    final ConsumerRecords<String, Dataset> records = consumer.poll(Duration.ofMillis(1000));

                    for (final ConsumerRecord<String, Dataset> record : records) {

                        final String key = record.key();
                        final Dataset value = record.value();
                        System.out.printf("---> Topic: %s, consumed Partition: %s, Offset: %d, Key: %s, Value: %s\n",
                                record.topic(), record.partition(), record.offset(), key, value);

                        linkedMap.put(key, value);
                    }
                }

                if(!linkedMap.isEmpty()){
                    //insert into db
                    MyDatabase.getDataFromConsumer(linkedMap);
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {


                if (consumer != null) {
                    consumer.close();
                }
            }
        };
    }

    /**
     * await 1seconds
     * @throws InterruptedException
     */
    public void stopConsuming() throws InterruptedException {
        doneConsuming = true;
        executorService.awaitTermination(1, TimeUnit.SECONDS);
        executorService.shutdownNow();
    }

    /**
     * thread sleep time is 2 seconds
     * @param args
     * @throws InterruptedException
     */
    public static void main(final String[] args) throws InterruptedException {

        MyConsumer myConsumerExample = new MyConsumer(2);
        myConsumerExample.startConsuming();
        // 30 seconds
        Thread.sleep(2000);
        myConsumerExample.stopConsuming();

    }

}
