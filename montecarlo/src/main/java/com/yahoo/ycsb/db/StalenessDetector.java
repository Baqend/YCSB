package com.yahoo.ycsb.db;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Michael Schaarschmidt.
 * <p>
 * This class keeps track of stale reads. To this end,
 * a client needs to register acknowledged write versions
 * as well as the timestamps of the write acknowledgement.
 */
public class StalenessDetector {
    private static volatile ConcurrentHashMap<String, Long> writeVersions = new ConcurrentHashMap<>();
    private static volatile ConcurrentHashMap<String, Long> writeAcknowledgments = new ConcurrentHashMap<>();
    private static AtomicInteger staleReads = new AtomicInteger();
    private static volatile AtomicInteger versionCounter = new AtomicInteger();

    public static long generateVersion() {
        return versionCounter.incrementAndGet();
    }

    /**
     * Adds a version as write-acknowledged, strictly monotonically increasing
     * per key.
     *
     * @param key
     * @param version
     */
    public static void addVersion(String key, long version) {
        writeVersions.compute(key, (k, v) -> {
            if (v == null) {
                return version;
            } else {
                return version > v ? version : v;
            }
        });
    }

    /**
     * Adds a version that is generated after a write has been
     * acknowledged, strictly monotonically increasing per key.
     *
     * @param key
     * @param version
     */
    public static void addWriteAcknowledgement(String key, long version) {
        writeAcknowledgments.compute(key, (k, v) -> {
            if (v == null) {
                return version;
            } else {
                return version > v ? version : v;
            }
        });
    }

    /**
     * Tests for stale reads in an atomic manner. By this definition, a
     * read is stale if the acknowledged write version is newer (larger) than
     * the version we are testing and if the read has begun after the
     * write has been acknowledged.
     *
     * @param key
     * @param version
     * @param readTimeStamp
     */
    public static void testForStaleness(String key, long version, long readTimeStamp) {
        writeVersions.compute(key, (k, v) -> {
            Long writeAck = writeAcknowledgments.get(key);

            if (v != null && writeAck != null && (v > version)
                    && (writeAck < readTimeStamp)) {
      //         System.out.println("stale read: version = " + version + ", v = " + v
      //                + ", readTimeStamp = " + readTimeStamp + ", writeAck = " + writeAck);

                staleReads.incrementAndGet();
                return v;
            } else {
                return version;
            }
        });
    }

    /**
     * Returns the overall count of stale reads.
     *
     * @return
     */
    public static long countStaleReads() {
        return staleReads.longValue();
    }

    public static void reset() {
        staleReads.set(0);
        writeAcknowledgments.clear();
        writeVersions.clear();
    }
}
