package com.yahoo.ycsb.db;

import com.yahoo.ycsb.SimulationResult;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.*;

import static java.util.AbstractMap.SimpleEntry;

/**
 * Created by Michael Schaarschmidt.
 */
public class CacheSimulator implements CacheLayer {

    private volatile SimulationLayer db;
    private volatile ConcurrentHashMap<String, DBObject> cache = new ConcurrentHashMap<>();
    private volatile ConcurrentHashMap<String, Long> purgedVersions = new ConcurrentHashMap<>();

    private volatile Map<String, StampedLock> locks = new ConcurrentHashMap<>();

    private AtomicInteger hits = new AtomicInteger();
    private AtomicInteger misses = new AtomicInteger();
    private AtomicInteger purges = new AtomicInteger();

    public CacheSimulator(SimulationLayer db) {
        this.db = db;
        ((DBSimulator) db).registerCache(this);
    }

    public DBObject read(String key) {
        try {
            StampedLock lock = locks.computeIfAbsent(key, k -> new StampedLock());
            DBObject obj = cacheLookup(key);

            if (obj == null) {
                long w = lock.writeLock();
                obj = cacheLookup(key);

                if (obj == null) {
                    misses.incrementAndGet();
                    obj = readFromDB(key);
                } else {
                    hits.incrementAndGet();
                }

                lock.unlock(w);
            } else {
                hits.incrementAndGet();
            }

            Thread.sleep(DistributionService.getClientToCacheSample());
            return obj;

        } catch (InterruptedException e) {
            e.printStackTrace();
            return null;
        }
    }

    private DBObject cacheLookup(String key) {
        DBObject obj;
        obj = cache.compute(key, (k, v) -> {
            if (v == null || v.getExpiration() < System.nanoTime()
                    || (purgedVersions.get(key) != null && v.getTimeStamp() < purgedVersions.get(key))) {
                return null;
            } else {
                return v;
            }
        });
        return obj;
    }


    public DBObject readFromDB(String key) {
        try {
            Thread.sleep(DistributionService.getCacheToDBSample());
            DBObject obj = db.read(key);

            cache.compute(key, (k, v) -> {
                if (purgedVersions.get(key) != null && purgedVersions.get(key) > obj.getTimeStamp()) {
                    return null;
                }
                if (v != null) {
                    return obj.getTimeStamp() > v.getTimeStamp() ? obj : v;
                }
                return obj;
            });
            return obj;
        } catch (InterruptedException e) {
            e.printStackTrace();
            return null;
        }
    }

    public void write(DBObject obj) {
        db.write(obj);
    }

    public void purge(DBObject obj) {
        cache.remove(obj.getKey());
        purgedVersions.put(obj.getKey(), obj.getTimeStamp());
    }

    public void addInvalidation() {
        purges.incrementAndGet();
    }

    public void printStatistics(String fileName) {
/*
        if (fileName != null) {
            try {
                BufferedWriter writer = new BufferedWriter(new FileWriter(fileName, true));
                writer.write("cache misses= " + misses.toString() + "\n");
                writer.write("cache hits= " + hits.toString() + "\n");
                writer.write("invalidations = " + purges.toString() + "\n");

                writer.write("hit_ratio=" + hits.doubleValue() / (hits.doubleValue() + misses.doubleValue()) + "\n");
                writer.flush();
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
*/
        System.out.println("cache hits = " + hits.toString());
        System.out.println("cache misses = " + misses.toString());
        System.out.println("invalidations = " + purges.toString());
        System.out.println("stale reads = " + StalenessDetector.countStaleReads());
        System.out.println("false positive p = " + ((DBSimulator) db).getFilter().getEstimatedFalsePositiveProbability());

        Double hitRatio = hits.doubleValue() / (hits.doubleValue() + misses.doubleValue());
        System.out.println("cache hit ratio = " + hitRatio);

    }

    public SimulationResult calculateScore() {
        return new SimulationResult(misses.longValue(), purges.longValue(), hits.longValue(),
                ((DBSimulator) db).getFilter().getEstimatedFalsePositiveProbability(), StalenessDetector.countStaleReads());
    }
}
