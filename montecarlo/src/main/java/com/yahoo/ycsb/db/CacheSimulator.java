package com.yahoo.ycsb.db;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Michael on 12.08.2014.
 */
public class CacheSimulator implements SimulationLayer {

    private volatile SimulationLayer db;
    private volatile ConcurrentHashMap<String, DBObject> cache = new ConcurrentHashMap<>();

    private AtomicInteger hits = new AtomicInteger();
    private AtomicInteger misses = new AtomicInteger();
    private AtomicInteger purges = new AtomicInteger();

    public CacheSimulator(SimulationLayer db) {
        this.db = db;
        ((DBSimulator) db).registerCache(this);
      }

    public DBObject read(String key) {
        DBObject obj = cache.compute(key, (k, v) -> v != null
                && v.getExpiration() < System.nanoTime() ? v : null);

        if (obj == null) {
            misses.incrementAndGet();
            return readFromDB(key);
        }
        else {
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
            cache.put(key, obj);
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

    public void purge(String key) {
        cache.remove(key);
        purges.incrementAndGet();
    }

    public void printStatistics() {
        System.out.println("cache hits = " + hits.toString());
        System.out.println("cache misses = " + misses.toString());
        System.out.println("invalidations = " + purges.toString());

        Double hitRatio = hits.doubleValue()/(hits.doubleValue() + misses.doubleValue());

        System.out.println("cache hit ratio = " + hitRatio);
    }
}
