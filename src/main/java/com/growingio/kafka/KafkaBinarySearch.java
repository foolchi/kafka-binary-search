package com.growingio.kafka;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by foolchi on 01/08/15.
 * Binary search for kafka message
 */
public class KafkaBinarySearch {

    private String topic;
    private List<PartitionMetadata> partitionMetadatas;
    private List<String> brokers;
    private int port;

    public KafkaBinarySearch(String topic, List<String> brokers, int port) {
        this.topic = topic;
        this.brokers = brokers;
        this.port = port;

        partitionMetadatas = getTopicMetadata();
        if (null == partitionMetadatas || partitionMetadatas.size() == 0) {
            System.out.println("Cannot find topic metadata");
        }
    }

    public long search(String dest, BinaryComparator comparator, int partition) {
        if (null == partitionMetadatas || partitionMetadatas.size() == 0) {
            System.out.println("Cannot find topic metadata");
            return -1;
        }

        for (PartitionMetadata partitionMetadata : partitionMetadatas) {
            if (partitionMetadata.partitionId() == partition) {
                long offset = search(partitionMetadata, dest, comparator);
                if (offset != -1)
                    return offset;
            }
        }
        return -1;
    }

    public long search(String dest, BinaryComparator comparator) {
        if (null == partitionMetadatas || partitionMetadatas.size() == 0) {
            System.out.println("Cannot find topic metadata");
            return -1;
        }

        for (PartitionMetadata partitionMetadata : partitionMetadatas) {
            long offset = search(partitionMetadata, dest, comparator);
            if (offset != -1)
                return offset;
        }

        return -1;
    }

    public long search(PartitionMetadata metadata, String dest, BinaryComparator comparator) {

        long startOffset = 0, maxOffset = getMaxOffset(metadata), middleOffset = (startOffset + maxOffset) / 2;

        String broker = metadata.leader().host();
        int port = metadata.leader().port();
        int partition = metadata.partitionId();
        String clientName = "Search_" + topic + "_" + metadata.partitionId();

        SimpleConsumer consumer = new SimpleConsumer(broker, port, 10000, 1024*1024, clientName);

        while (startOffset <= maxOffset) {
            System.out.println("offset: " + startOffset + "," + middleOffset + "," + maxOffset);
            FetchRequest request = new FetchRequestBuilder()
                    .clientId(clientName)
                    .addFetch(topic, partition, middleOffset, 1000)
                    .build();
            FetchResponse fetchResponse = consumer.fetch(request);

            for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partition)) {
                ByteBuffer payload = messageAndOffset.message().payload();
                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);
                String msg = null;
                try {
                    msg = new String(bytes, "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                    continue;
                }

                System.out.println(msg);
                int compareResult = comparator.compare(msg, dest);
                if (compareResult == 0) {
                    consumer.close();
                    return middleOffset;
                } else if (compareResult < 0) {
                    startOffset = middleOffset + 1;
                } else {
                    maxOffset = middleOffset - 1;
                }
                middleOffset = (startOffset + maxOffset) / 2;
                break;
            }
        }
        consumer.close();
        return -1;
    }

    public long fuzzySearch(String dest, FuzzyBinaryComparator comparator) {
        if (null == partitionMetadatas || partitionMetadatas.size() == 0) {
            System.out.println("Cannot find topic metadata");
            return -1;
        }

        for (PartitionMetadata partitionMetadata : partitionMetadatas) {
            long offset = fuzzySearch(partitionMetadata, dest, comparator);
            if (offset != -1)
                return offset;
        }
        return -1;
    }

    public long fuzzySearch(PartitionMetadata metadata, String dest, FuzzyBinaryComparator comparator) {
        long middleOffset = search(metadata, dest, comparator);
        if (middleOffset == -1)
            return middleOffset;

        long leftOffset = sequenceSearch(metadata, dest, comparator, middleOffset, -1);
        if (leftOffset != -1)
            return leftOffset;
        return sequenceSearch(metadata, dest, comparator, middleOffset + 1, 1);
    }

    public long sequenceSearch(PartitionMetadata metadata, String dest, FuzzyBinaryComparator comparator, long startOffset, int step) {
        long maxOffset = getMaxOffset(metadata);

        String broker = metadata.leader().host();
        int port = metadata.leader().port();
        int partition = metadata.partitionId();
        String clientName = "Search_" + topic + "_" + metadata.partitionId();

        SimpleConsumer consumer = new SimpleConsumer(broker, port, 10000, 1024*1024, clientName);

        while (startOffset >= 0 && startOffset <= maxOffset) {
            FetchRequest request = new FetchRequestBuilder()
                    .clientId(clientName)
                    .addFetch(topic, partition, startOffset, 1000)
                    .build();
            FetchResponse fetchResponse = consumer.fetch(request);
            for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partition)) {
                ByteBuffer payload = messageAndOffset.message().payload();
                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);
                String msg = null;
                try {
                    msg = new String(bytes, "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
                System.out.println(msg);
                if (comparator.exactCompare(msg, dest) == 0) {
                    consumer.close();
                    return startOffset;
                }

                if (comparator.compare(msg, dest) * step > 0) {
                    consumer.close();
                    return -1;
                }
                break;
            }
            startOffset += step;
        }

        consumer.close();
        return -1;
    }


    public long getMaxOffset(PartitionMetadata metadata) {
        String broker = metadata.leader().host();
        int port = metadata.leader().port();
        String clientName = "getMaxOffset_" + topic + "_" + metadata.partitionId();

        SimpleConsumer consumer = new SimpleConsumer(broker, port, 10000, 1024*1024, clientName);

        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, metadata.partitionId());
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), 1));
        OffsetRequest request = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);

        long[] offsets = response.offsets(topic, metadata.partitionId());

        consumer.close();

        return offsets[0];
    }


    public List<PartitionMetadata> getTopicMetadata() {
        Broker leader = findLeader();
        System.out.println(leader.toString());

        SimpleConsumer consumer = new SimpleConsumer(leader.host(), leader.port(), 10000, 1024*1024, "getTopicMetadata");
        List<String> topics = new ArrayList<String>();
        topics.add(topic);
        TopicMetadataRequest req = new TopicMetadataRequest(topics);
        TopicMetadataResponse resp = consumer.send(req);

        List<TopicMetadata> metadatas = resp.topicsMetadata();
        if (null != metadatas && metadatas.size() >= 1) {
            consumer.close();
            return metadatas.get(0).partitionsMetadata();
        }

        consumer.close();

        return null;
    }

    public Broker findLeader() {
        Broker leader;

        for (String broker : brokers) {
            SimpleConsumer consumer = new SimpleConsumer(broker, port, 10000, 1024*1024, "find_leader");
            List<String> topics = new ArrayList<String>();
            topics.add(topic);
            TopicMetadataRequest req = new TopicMetadataRequest(topics);
            TopicMetadataResponse resp = consumer.send(req);

            List<TopicMetadata> metaDatas = resp.topicsMetadata();
            for (TopicMetadata mdata : metaDatas) {
                for (PartitionMetadata part : mdata.partitionsMetadata()) {
                    leader = part.leader();
                    if (null != leader) {
                        return leader;
                    }
                }
            }
        }

        return null;
    }

}
