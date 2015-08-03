package com.growingio.kafka

/**
 * Created by foolchi on 03/08/15.
 * 带模糊匹配的时间戳比较类
 * maxDiff是默认的最大误差(+/-)
 */
class TimestampFuzzyComparator (dest : String, val maxDiff : Long)extends FuzzyBinaryComparator{
  private val destTime = dest.toLong

  override def exactCompare(msg: String): Int = {
    val diff = msg.toLong - destTime
    if (diff > 0) 1 else if (diff < 0) -1 else 0
  }

  override def compare(msg: String): Int = {
    val diff = msg.toLong - destTime
    if (diff.abs < maxDiff) 0 else if (diff < 0) -1 else 1
  }
}
