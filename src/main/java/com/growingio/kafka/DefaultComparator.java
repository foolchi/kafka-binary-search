package com.growingio.kafka;

/**
 * Created by foolchi on 01/08/15.
 */
public class DefaultComparator implements BinaryComparator {
    public int compare(String msg, String dest) {
        return msg.compareTo(dest);
    }
}
