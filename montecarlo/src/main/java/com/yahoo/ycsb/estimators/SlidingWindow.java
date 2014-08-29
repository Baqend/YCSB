package com.yahoo.ycsb.estimators;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Created by Michael on 19.08.2014.
 */
public abstract class SlidingWindow implements ReadWriteCounter {

    private final double timeWindow;

    private volatile ConcurrentHashMap<String, Double> readFrequency = new ConcurrentHashMap<>();
    private volatile ConcurrentHashMap<String, ConcurrentLinkedDeque<Long>> readArrivals = new ConcurrentHashMap<>();
    private volatile ConcurrentHashMap<String, ConcurrentLinkedDeque<Long>> writeArrivals = new ConcurrentHashMap<>();
    private volatile ConcurrentHashMap<String, Double> writeFrequency = new ConcurrentHashMap<>();

    /**
     * @param timeWindow Estimation window in s.
     */
    public SlidingWindow(double timeWindow) {

        // internal calculation requires nanoseconds
        this.timeWindow = timeWindow * 1_000_000_000l;
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
        updateWriteFrequency(key);
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

        updateReadFrequency(key);
    }

    private void updateReadFrequency(String key) {
        ConcurrentLinkedDeque<Long> reads = readArrivals.get(key);
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
            if (k == 1) {
                readFrequency.put(key, 1 / (double) (now - reads.getLast()));
            } else {
                Double f = k / timeWindow;
                readFrequency.put(key, f);
            }
        }
    }

    private void updateWriteFrequency(String key) {
        ConcurrentLinkedDeque<Long> writes = writeArrivals.get(key);

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
            if (k == 1) {
                writeFrequency.put(key, 1 / (double) (now - writes.getLast()));
            } else {
                Double f = k / timeWindow;
                writeFrequency.put(key, f);
            }
        }

    }

    @Override
    public double getReadFrequency(String key) {
        return readFrequency.get(key);
    }

    @Override
    public double getWriteFrequency(String key) {
        return writeFrequency.get(key);
    }

}
