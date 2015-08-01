package com.growingio.kafka;

/**
 * Created by foolchi on 01/08/15.
 */
public class TimestampComparator implements BinaryComparator {
    public int compare(String msg, String dest) {
        long diff = Long.parseLong(msg) - Long.parseLong(dest);
        if (diff > 0)
            return 1;
        if (diff < 0)
            return -1;
        return 0;
    }
}
