package com.growingio.kafka

import kafka.api._
import kafka.cluster.Broker
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer

/**
 * Created by foolchi on 03/08/15.
 * Kafka二分查找
 */
class KafkaBinarySearch (topic : String, host : String, port : Int) extends KafkaSearch(topic, host, port){

  // 二分查找
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
    var start = getOffset(metadata, OffsetRequest.EarliestTime)
    var end = getOffset(metadata, OffsetRequest.LatestTime)
    var middle = (start + end) / 2

    println(end)

    val broker = metadata.leader.get
    println(broker)
    val partition = metadata.partitionId
    val clientName = "Search_" + topic + "_" + partition
    val consumer = new SimpleConsumer(broker.host, broker.port, 10000, 64 * 1024, clientName)

    while (start <= end) {
      val request = new FetchRequestBuilder()
        .clientId(clientName)
        .addFetch(topic, partition, middle, 1000)
        .build()
      val fetchResponse = consumer.fetch(request)
      println(start, end)

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

  // 模糊查询
  def fuzzySearch(comparator: FuzzyBinaryComparator): Long = {
    if (partitionMetadatas == null || partitionMetadatas.isEmpty) {
      println("Cannot find topic metadata")
      return -1
    }

    for (partitionMetadata <- partitionMetadatas) {
      val offset = fuzzySearch(partitionMetadata, comparator)
      if (offset != -1)
        return offset
    }

    -1
  }

  def fuzzySearch(metadata: PartitionMetadata, comparator: FuzzyBinaryComparator) : Long = {
    // 首先使用二分查找找到误差范围内的某个offset
    val middleOffset = search(metadata, comparator)
    if (middleOffset == -1)
      return -1

    // 根据此offset向左右按顺序查找
    val rightOffset = sequenceSearch(metadata, comparator, middleOffset, 1)
    if (rightOffset != -1)
      return rightOffset
    sequenceSearch(metadata, comparator, middleOffset, -1)
  }

  // 从start开始按顺序查找
  // step = 1向右，-1向左
  def sequenceSearch(metadata: PartitionMetadata, comparator: FuzzyBinaryComparator, start : Long, step : Long) : Long = {
    var current = start
    val end = getOffset(metadata, OffsetRequest.LatestTime)

    println(end)
    val broker = metadata.leader.get
    println(broker)
    val partition = metadata.partitionId
    val clientName = "Search_" + topic + "_" + partition

    val consumer = new SimpleConsumer(broker.host, broker.port, 10000, 64 * 1024, clientName)

    while (current >= 0 && current <= end) {
      val request = new FetchRequestBuilder()
        .clientId(clientName)
        .addFetch(topic, partition, current, 1000)
        .build()
      val fetchResponse = consumer.fetch(request)

      val messageAndOffset = fetchResponse.messageSet(topic, partition).head
      val payload = messageAndOffset.message.payload
      val bytes = new Array[Byte](payload.limit())
      payload.get(bytes)
      val msg = new String(bytes, "UTF-8")

      if (comparator.exactCompare(msg) == 0) {
        consumer.close()
        return current
      }

      if (comparator.compare(msg) * step > 0) {
        consumer.close()
        return -1
      }

      current += step
    }

    consumer.close()
    -1
  }
}
