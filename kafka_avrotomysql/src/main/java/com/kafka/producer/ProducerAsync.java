package com.kafka.producer;

import com.kafka.MyTest;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerAsync {
    public static void main(String[] args) {
        Properties props = new Properties(); //Properties 오브젝트를 시작합니다.
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092"); //브로커 리스트를 정의합니다.
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class); //메시지 키와 벨류에 문자열을 지정하므로 내장된 StringSerializer를 지정합니다.
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        Producer<String, String> producer = new KafkaProducer<>(props); //Properties 오브젝트를 전달해 새 프로듀서를 생성합니다.

//        Field_Type fieldType = new Field_Type();
        MyTest test = new MyTest() ;

        try {
            for (int i = 0; i < 3; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>("testTopic", "test - " + i); //ProducerRecord 오브젝트를 생성합니다.
                producer.send(record, new ProducerCallback(record)); //프로듀서에서 레코드를 보낼 때 콜백 오브젝트를 같이 보냅니다.
            }
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            producer.close(); // 프로듀서 종료
        }
    }
}