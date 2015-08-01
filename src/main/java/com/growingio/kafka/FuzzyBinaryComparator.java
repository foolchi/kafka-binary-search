package com.growingio.kafka;

/**
 * Created by foolchi on 01/08/15.
 */
public interface FuzzyBinaryComparator extends BinaryComparator {
    int exactCompare(String msg, String dest);
}
