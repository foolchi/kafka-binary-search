package com.growingio.kafka

import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo, TopicMetadataRequest, PartitionMetadata}
import kafka.cluster.Broker
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer

/**
 * Created by qifu on 15/10/19.
 */
class KafkaSearch (val topic: String, val host: String, val port: Int){
  val topics = Array(topic)
  val partitionMetadatas = getTopicMetadata


  def findLeader() : Broker = {
    val consumer = new SimpleConsumer(host, port, 10000, 64 * 1024, "find_leader")
    val resp = consumer.send(new TopicMetadataRequest(topics, 1)) // 1是correlationId，用来匹配client和server
    consumer.close()

    val metaDatas = resp.topicsMetadata
    for (mdata <- metaDatas) {
      for (part <- mdata.partitionsMetadata) {
        val leader = part.leader
        if (leader.isDefined) {
          return leader.get
        }
      }
    }

    null
  }


  def getTopicMetadata() : Seq[PartitionMetadata] = {
    val leader = findLeader()
    val consumer = new SimpleConsumer(leader.host, leader.port, 10000, 64 * 1024, "getTopicMetadata")
    val resp = consumer.send(new TopicMetadataRequest(topics, 77))
    consumer.close()

    val metaDatas = resp.topicsMetadata
    if (metaDatas != null && metaDatas.nonEmpty) {
      return metaDatas.head.partitionsMetadata
    }

    null
  }

  def getOffset(metadata: PartitionMetadata, offset: Long) : Long = {
    val broker = metadata.leader.get
    val clientName = "getMaxOffset_" + topic + "_" + metadata.partitionId

    val consumer = new SimpleConsumer(broker.host, broker.port, 10000, 64 * 1024, clientName)
    val topicAndPartition = new TopicAndPartition(topic, metadata.partitionId)
    val requestInfo = Map(topicAndPartition -> new PartitionOffsetRequestInfo(offset, 1))
    val request = new OffsetRequest(requestInfo, OffsetRequest.CurrentVersion, clientId = clientName)
    val response = consumer.getOffsetsBefore(request)
    response.partitionErrorAndOffsets
      .get(topicAndPartition)
      .get.offsets.last
  }
}
