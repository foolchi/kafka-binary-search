package com.growingio.kafka

/**
 * Created by foolchi on 03/08/15.
 * 简单的时间戳比较类
 */
class TimestampComparator (dest : String) extends BinaryComparator{
  private val destTime = dest.toLong

  override def compare(msg: String): Int = {
    val diff = msg.toLong - destTime
    if (diff > 0) 1 else if (diff < 0) -1 else 0
  }
}
