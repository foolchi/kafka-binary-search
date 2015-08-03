package com.growingio.kafka

/**
 * Created by foolchi on 03/08/15.
 * 二分查找接口
 */

trait BinaryComparator {
  // 返回比较结果
  // 正数表示当前结果大于目标，负数表示小于，0表示等于
  def compare(msg : String) : Int
}
