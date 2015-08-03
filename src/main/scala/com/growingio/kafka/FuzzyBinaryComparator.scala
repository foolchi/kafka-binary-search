package com.growingio.kafka

/**
 * Created by foolchi on 03/08/15.
 * 模糊搜索接口
 */

trait FuzzyBinaryComparator extends BinaryComparator {
  // 返回精确比较结果
  // 正数表示当前结果大于目标，负数表示小于，0表示等于
  def exactCompare(msg : String) : Int
}

