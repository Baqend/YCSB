package com.yahoo.ycsb.estimators;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Created by Michael on 19.08.2014.
 */
public class SlidingWindow implements ReadWriteCounter {

    private final double timeWindow;

    //TODO: notice when sliding window has passed once for each member
    private volatile ConcurrentHashMap<String, ConcurrentLinkedDeque<Long>> readArrivals = new ConcurrentHashMap<>();
    private volatile ConcurrentHashMap<String, ConcurrentLinkedDeque<Long>> writeArrivals = new ConcurrentHashMap<>();
    private volatile long start;

    /**
     * @param timeWindow Estimation window in s.
     */
    public SlidingWindow(double timeWindow) {

        // internal calculation requires nanoseconds
        this.timeWindow = timeWindow * 1_000_000_000l;
        start = System.nanoTime();
    }

    @Override
    public void registerWrite(String key) {
        writeArrivals.compute(key, (k, v) -> {
            if (v == null) {
                v = new ConcurrentLinkedDeque<>();
            }
            v.offer(System.nanoTime());
            return v;
        });
    }

    @Override
    public void registerRead(String key) {
        readArrivals.compute(key, (k, v) -> {
            if (v == null) {
                v = new ConcurrentLinkedDeque<>();
            }
            v.offer(System.nanoTime());
            return v;
        });
    }

    @Override
    public Double getReadFrequency(String key) {
        ConcurrentLinkedDeque<Long> reads = readArrivals.get(key);

        if (reads != null) {
            long now = System.nanoTime();
            long cutOff = now - (long) timeWindow;

            Iterator<Long> it = reads.iterator();

            // Only consider reads in time window
            while (it.hasNext()) {
                if (it.next() < cutOff) {
                    it.remove();
                }
            }

            int k = reads.size();
            if (k > 0) {
                if (now - reads.getFirst() < timeWindow) {
                    long adjustedWindow = now - start;
                    return k / (double) adjustedWindow;

                } else {
                    return k / timeWindow;
                }
            }
        }
        return null;
    }

    @Override
    public Double getWriteFrequency(String key) {

        ConcurrentLinkedDeque<Long> writes = writeArrivals.get(key);
        if (writes != null) {
            long now = System.nanoTime();
            long cutOff = now - (long) timeWindow;

            Iterator<Long> it = writes.iterator();

            // Only consider writes in time window
            while (it.hasNext()) {
                if (it.next() < cutOff) {
                    it.remove();
                }
            }

            int k = writes.size();
            if (k > 0) {
                if (now - writes.getFirst() < timeWindow) {
                    long adjustedWindow = now - start;
                    return k / (double) adjustedWindow;
                } else {
                    return k / timeWindow;
                }
            }
        }
        return null;
    }

}
