package com.yahoo.ycsb.estimators;

import com.yahoo.ycsb.db.DistributionService;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Created by Michael on 19.08.2014.
 */
public class PoissonEstimator implements TTLEstimator {

    private final double base;
    private final long maxTtl;
    private final double timeWindow;
    private final int scaling = DistributionService.getScalingFactor();

    private volatile ConcurrentHashMap<String, Double> readFrequency = new ConcurrentHashMap<>();
    private volatile ConcurrentHashMap<String, ConcurrentLinkedDeque<Long>> readArrivals = new ConcurrentHashMap<>();
    private volatile ConcurrentHashMap<String, ConcurrentLinkedDeque<Long>> writeArrivals = new ConcurrentHashMap<>();
    private volatile ConcurrentHashMap<String, Double> writeFrequency = new ConcurrentHashMap<>();

    /**
     * @param base       Base for exponential calculation.
     * @param maxTtl     Maximum of ttl in s.
     * @param timeWindow Estimation window in s.
     */
    public PoissonEstimator(double base, long maxTtl, double timeWindow) {
        this.base = base;

        // internal calculation requires nanoseconds
        this.maxTtl = maxTtl * 1_000_000_000l;
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
    public long registerRead(String key) {
        readArrivals.compute(key, (k, v) -> {
            if (v == null) {
                v = new ConcurrentLinkedDeque<>();
            }
            v.offer(System.nanoTime());
            return v;
        });

        updateReadFrequency(key);
        long ttl = estimate(key);
        System.out.println("unscaled ttl for  = " + key + " is = " + ttl + "ns");

        return ttl / scaling;
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
    public long estimate(String key) {
        Double readF = readFrequency.get(key);
        Double writeF = writeFrequency.get(key);

        return 0;
    }
}
