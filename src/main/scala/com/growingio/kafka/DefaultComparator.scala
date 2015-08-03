package com.growingio.kafka

/**
 * Created by foolchi on 03/08/15.
 * 默认String比较类
 */
class DefaultComparator(dest : String) extends BinaryComparator{
  override def compare(msg: String): Int = msg.compareTo(dest)
}
