package com.growingio.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.util.*;

/**
 * Created by foolchi on 01/08/15.
 * Unit test
 */
public class BinarySearchTest {

    @Test
    public void testBinarySearch() {
        String topic = "binary_search_test_" + System.currentTimeMillis();

        // create data
        long message_size = 1000;
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        Map<String, Long> msgOffsets = new HashMap<String, Long>();

        for (long i = 0; i < message_size; i++) {
            String msg = System.currentTimeMillis() + "";
            producer.send(new ProducerRecord<String, String>(topic, msg));
            msgOffsets.put(msg, i);
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // 验证查找结果
        String[] brokers = new String[]{"localhost"};
        int port = 9092;
        KafkaBinarySearch binarySearch = new KafkaBinarySearch(topic, brokers, port);
        Iterator<Map.Entry<String, Long>> iterator = msgOffsets.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry entry = iterator.next();
            String key = (String) entry.getKey();
            long offset = (Long) entry.getValue();
            assertEquals("Wrong offset: " + key, offset, binarySearch.search(new TimestampComparator(key)));
        }
    }

    @Test
    public void testFuzzyBinarySearch() {
        String topic = "binary_search_test_" + System.currentTimeMillis();
        int disturbance = 10; // 随机扰动范围
        Random r = new Random(System.currentTimeMillis());

        // create data
        long message_size = 100;
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        Map<String, Long> msgOffsets = new HashMap<String, Long>();

        for (long i = 0; i < message_size; ) {
            long currentTime = System.currentTimeMillis() + r.nextInt(disturbance);
            String msg = currentTime + "";
            if (!msgOffsets.containsKey(msg)) {
                producer.send(new ProducerRecord<String, String>(topic, msg));
                msgOffsets.put(msg, i);
                i++;
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // 验证查找结果
        String[] brokers = new String[]{"localhost"};
        int port = 9092;

        KafkaBinarySearch binarySearch = new KafkaBinarySearch(topic, brokers, port);
        Iterator<Map.Entry<String, Long>> iterator = msgOffsets.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry entry = iterator.next();
            String key = (String) entry.getKey();
            long offset = (Long) entry.getValue();
//            assertEquals("Wrong offset" + key, offset, binarySearch.fuzzySearch(new TimestampFuzzyComparator(key, disturbance)));
        }
    }

}
