package com.growingio.kafka

import kafka.api._
import kafka.cluster.Broker
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer

/**
 * Created by foolchi on 03/08/15.
 * Kafka二分查找
 */
class KafkaBinarySearch (val topic : String, val hosts : Array[String], val port : Int){
  private val topics = Array{topic}
  private val partitionMetadatas = getTopicMetadata

  def search(comparator: BinaryComparator) : Long = {
    if (partitionMetadatas == null || partitionMetadatas.isEmpty) {
      println("Cannot find topic metadata")
      return -1
    }

    for (partitionMetadata <- partitionMetadatas) {
      val offset = search(partitionMetadata, comparator)
      if (offset != -1)
        return offset
    }

    -1
  }

  def search(metadata: PartitionMetadata, comparator: BinaryComparator) : Long = {
    var start = 0L
    var end = getMaxOffset(metadata)
    var middle = (start + end) / 2

    val broker = metadata.leader.get
    val partition = metadata.partitionId
    val clientName = "Search_" + topic + "_" + partition
    val consumer = new SimpleConsumer(broker.host, broker.port, 10000, 64 * 1024, clientName)

    while (start <= end) {
      val request = new FetchRequestBuilder()
        .clientId(clientName)
        .addFetch(topic, partition, middle, 1000)
        .build()
      val fetchResponse = consumer.fetch(request)

      val messageAndOffset = fetchResponse.messageSet(topic, partition).head
      val payload = messageAndOffset.message.payload
      val bytes = new Array[Byte](payload.limit())
      payload.get(bytes)
      val msg = new String(bytes, "UTF-8")

      val compareResult = comparator.compare(msg)
      if (compareResult == 0) {
        consumer.close()
        return middle
      } else if (compareResult < 0) {
        start = middle + 1
      } else {
        end = middle - 1
      }
      middle = (start + end) / 2
    }

    consumer.close()
    -1
  }

  def getMaxOffset(metadata: PartitionMetadata) : Long = {
    val broker = metadata.leader.get
    val clientName = "getMaxOffset_" + topic + "_" + metadata.partitionId

    val consumer = new SimpleConsumer(broker.host, broker.port, 10000, 64 * 1024, clientName)
    val topicAndPartition = new TopicAndPartition(topic, metadata.partitionId)
    val requestInfo = Map(topicAndPartition -> new PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1))
    val request = new OffsetRequest(requestInfo, OffsetRequest.CurrentVersion, clientId = clientName)
    val response = consumer.getOffsetsBefore(request)
    response.partitionErrorAndOffsets
      .get(topicAndPartition)
      .get.offsets.head
  }


  def findLeader() : Broker = {
    for (host <- hosts) {
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
    }

    null
  }

  def getTopicMetadata: Seq[PartitionMetadata] = {
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
}
