package com.growingio.kafka;

/**
 * Created by foolchi on 01/08/15.
 */
public interface BinaryComparator {
    int compare(String msg, String dest);
}
