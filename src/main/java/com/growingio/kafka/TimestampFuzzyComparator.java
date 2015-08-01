package com.growingio.kafka;

/**
 * Created by foolchi on 01/08/15.
 */
public class TimestampFuzzyComparator implements FuzzyBinaryComparator {
    private long maxDiff;

    public TimestampFuzzyComparator(long maxDiff) {
        this.maxDiff = maxDiff;
    }

    public int exactCompare(String msg, String dest) {
        long diff = Long.parseLong(msg) - Long.parseLong(dest);
        if (diff > 0)
            return 1;
        if (diff < 0)
            return -1;
        return 0;
    }

    public int compare(String msg, String dest) {
        long diff = Long.parseLong(msg) - Long.parseLong(dest);
        if (Math.abs(diff) < maxDiff) {
            return 0;
        }
        if (diff < 0)
            return -1;
        return 1;
    }
}
