package com.yahoo.ycsb.db;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Michael on 07.08.2014.
 */
public class StalenessDetector {
    private static ConcurrentHashMap<String, Long> versions = new ConcurrentHashMap<>();
    private static AtomicInteger staleReads = new AtomicInteger();

    public static long generateVersion() {
        return System.nanoTime();
    }

    public static void addVersion(String key, Long version) {
        versions.compute(key, (k, v) ->
                v != null && version > v ? version : v);
    }

    public static void testForStaleness(String key, Long version, Long readTimeStamp) {
        versions.compute(key, (k, v) -> {
            if (v != null && (v > version) && (readTimeStamp > v)) {
                System.out.println("version = " + version);
                staleReads.incrementAndGet();
                return v;
            }
            else {
                return version;
            }
        });
    }

    public static long countStaleReads() {
        return staleReads.longValue();
    }
}
