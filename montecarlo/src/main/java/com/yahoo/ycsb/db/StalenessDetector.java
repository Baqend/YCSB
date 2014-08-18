package com.yahoo.ycsb.db;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Michael on 07.08.2014.
 */
public class StalenessDetector {
    private static volatile ConcurrentHashMap<String, Long> writeVersions = new ConcurrentHashMap<>();
    private static volatile ConcurrentHashMap<String, Long> writeAcknowledgments = new ConcurrentHashMap<>();
    private static AtomicInteger staleReads = new AtomicInteger();
    private static volatile AtomicInteger versionCounter = new AtomicInteger();

    public static long generateVersion() {
        return versionCounter.incrementAndGet();
    }

    public static void addVersion(String key, long version) {
        writeVersions.compute(key, (k, v) -> {
            if (v == null) {
                return version;
            } else {
                return version > v ? version : v;
            }
        });
    }

    public static void addWriteAcknowledgement(String key, long version) {
        writeAcknowledgments.compute(key, (k, v) -> {
            if (v == null) {
                return version;
            } else {
                return version > v ? version : v;
            }
        });
    }

    public static void testForStaleness(String key, long version, long readTimeStamp) {
        writeVersions.compute(key, (k, v) -> {
            Long writeAck = writeAcknowledgments.get(key);
            if (v != null && writeAck != null && (v > version)
                    && (writeAck < readTimeStamp)) {
                System.out.println("stale read: version = " + version + ", v = " + v
                + ", readTimeStamp = " + readTimeStamp + ", writeAck = " + writeAck);

                staleReads.incrementAndGet();
                return v;
            } else {
                return version;
            }
        });
    }

    public static long countStaleReads() {
        return staleReads.longValue();
    }
}
