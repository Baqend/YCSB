package com.yahoo.ycsb.db;

import com.yahoo.ycsb.estimators.TTLEstimator;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by Michael Schaarschmidt.
 * <p>
 * This class simulates a database by both managing versions (which does not differ
 * from a cache layer) and by carrying out invalidations to the cache layer
 * it is connected to.
 * <p>
 * Note that invalidations are carried out asynchronously after the
 * invalidation latency by the ScheduledExecutorService. Obviously,
 * a greater invalidation-latency will result in more stale-reads, but also
 * more cache-hits.
 */
public class DBSimulator implements SimulationLayer {

    private volatile ConcurrentHashMap<String, DBObject> db = new ConcurrentHashMap<>();
    private volatile TTLEstimator estimator;
    private volatile CacheLayer cache;

    private volatile ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

    public DBSimulator(TTLEstimator estimator) {
        this.estimator = estimator;
    }

    @Override
    public DBObject read(String key) {
        DBObject obj = db.get(key);

        if (obj == null) {
            obj = new DBObject(key, 0);
        }

        // Everytime an object is read, we need a new TTL estimation.
        obj.setExpiration(System.nanoTime() + estimator.registerRead(key));

        try {
            Thread.sleep(DistributionService.getCacheToDBSample());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return obj;
    }


    @Override
    public void write(DBObject obj) {
        try {
            db.compute(obj.getKey(), (k, v) -> {
                if (v == null) {
                    return obj;
                } else {
                    return obj.getTimeStamp() > v.getTimeStamp() ? obj : v;
                }
            });
            estimator.registerWrite(obj.getKey());
            // Schedule an invalidation for the version we just wrote to the database.
            executorService.schedule(
                    () -> cache.purge(obj), DistributionService.getPurgeSample(),
                    TimeUnit.MILLISECONDS);

            Thread.sleep(DistributionService.getDBWriteSample()
                    + DistributionService.getCacheToDBSample());


        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Registers a Cache for purging purposes.
     *
     * @param cacheSimulator
     */
    public void registerCache(CacheLayer cacheSimulator) {
        cache = cacheSimulator;
    }
}
