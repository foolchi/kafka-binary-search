package com.growingio.kafka

import collection.JavaConversions._
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.Test
import org.junit.Assert.assertEquals

/**
 * Created by foolchi on 04/08/15.
 * Unit test
 */

class BinarySearchTest {

  @Test
  def testBinarySearch() = {
    val topic = "binary_search_test_" + System.currentTimeMillis

    // create data
    val message_size = 1000
    val props = Map(
      "bootstrap.servers" -> "localhost:9092",
      "acks" -> "1",
    "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
    )
    val producer = new KafkaProducer[String, String](props)
    val msgOffsets = new scala.collection.mutable.HashMap[String, Long]()

    for (i <- 0 until message_size) {
      val msg = System.currentTimeMillis().toString
      producer.send(new ProducerRecord[String, String](topic, msg))
      msgOffsets(msg) = i
      Thread.sleep(1)
    }

    val broker = "localhost"
    val port = 9092
    val binarySearch = new KafkaBinarySearch(topic, broker, port)

    msgOffsets.foreach{
      case (key, value) =>
      assertEquals("Wrong offset: " + key, value, binarySearch.search(new TimestampComparator(key)))
    }
  }

  @Test
  def testFuzzyBinarySearch() = {
    val topic = "binary_search_test_" + System.currentTimeMillis
    val disturbance = 10 // 随机扰动范围
    val r = scala.util.Random

    // create data
    val message_size = 100
    val props = Map(
      "bootstrap.servers" -> "localhost:9092",
      "acks" -> "1",
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
    )
    val producer = new KafkaProducer[String, String](props)
    val msgOffsets = new scala.collection.mutable.HashMap[String, Long]()
    var i = 0

    while (i < message_size) {
      val msg = (System.currentTimeMillis + r.nextInt(disturbance)).toString
      if (!msgOffsets.contains(msg)) {
        producer.send(new ProducerRecord[String, String](topic, msg))
        msgOffsets(msg) = i
        i += 1
      }
      Thread.sleep(1)
    }

    val broker = "localhost"
    val port = 9092
    val binarySearch = new KafkaBinarySearch(topic, broker, port)

    msgOffsets.foreach{
      case (key, value) =>
        assertEquals("Wrong offset: " + key, value, binarySearch.fuzzySearch(new TimestampFuzzyComparator(key, disturbance)))
    }
  }

}
