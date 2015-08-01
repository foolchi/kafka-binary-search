package com.growingio.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;

/**
 * Created by foolchi on 01/08/15.
 * Only send messages with timestamps
 */
public class SimpleKafkaProducer {
    public static void main(String[] args) throws Exception {
        String topic = "test1";

        int message_size = 1000;

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < message_size; i++) {
            producer.send(new ProducerRecord<String, String>(topic, System.currentTimeMillis() + ""));
            Thread.sleep(1);
        }
    }
}
