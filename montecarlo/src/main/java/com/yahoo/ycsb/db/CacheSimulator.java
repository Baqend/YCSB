package com.yahoo.ycsb.db;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Michael Schaarschmidt.
 */
public class CacheSimulator implements CacheLayer {

    private volatile SimulationLayer db;
    private volatile ConcurrentHashMap<String, DBObject> cache = new ConcurrentHashMap<>();
    private volatile ConcurrentHashMap<String, Long> purgedVersions = new ConcurrentHashMap<>();

    private AtomicInteger hits = new AtomicInteger();
    private AtomicInteger misses = new AtomicInteger();
    private AtomicInteger purges = new AtomicInteger();

    public CacheSimulator(SimulationLayer db) {
        this.db = db;
        ((DBSimulator) db).registerCache(this);
    }

    public DBObject read(String key) {
        DBObject obj = cache.compute(key, (k, v) -> {
            if (v == null || v.getExpiration() < System.nanoTime()
                    || (purgedVersions.get(key) != null && v.getTimeStamp() < purgedVersions.get(key))) {
                return null;
            } else {
                return v;
            }
        });

        if (obj == null) {
            misses.incrementAndGet();
            return readFromDB(key);
        } else {
            try {
                Thread.sleep(DistributionService.getClientToCacheSample());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            hits.incrementAndGet();
            return obj;
        }
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
            Thread.sleep(DistributionService.getClientToCacheSample());

            return obj;
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void write(DBObject obj) {
        try {
            Thread.sleep(DistributionService.getCacheToDBSample());
            db.write(obj);
            Thread.sleep(DistributionService.getClientToCacheSample());

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void purge(DBObject obj) {
        cache.remove(obj.getKey());
        purgedVersions.put(obj.getKey(), obj.getTimeStamp());
        purges.incrementAndGet();
    }

    public void printStatistics(String fileName) {
        if (fileName != null) {
            try {
                BufferedWriter writer = new BufferedWriter(new FileWriter(fileName, true));
                writer.write("cache misses= " + misses.toString() + "\n");
                writer.write("cache hits= " + hits.toString() + "\n");
                writer.write("invalidations hits= " + purges.toString() + "\n");
                writer.write("hit_ratio=" + hits.doubleValue() / (hits.doubleValue() + misses.doubleValue()) + "\n");
                writer.flush();
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        System.out.println("cache hits = " + hits.toString());
        System.out.println("cache misses = " + misses.toString());
        System.out.println("invalidations = " + purges.toString());

        Double hitRatio = hits.doubleValue() / (hits.doubleValue() + misses.doubleValue());

        System.out.println("cache hit ratio = " + hitRatio);
    }


}
