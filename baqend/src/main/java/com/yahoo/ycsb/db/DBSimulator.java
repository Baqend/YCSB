package com.yahoo.ycsb.db;

import com.yahoo.ycsb.estimators.TTLEstimator;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by Michael on 12.08.2014.
 */
public class DBSimulator implements SimulationLayer {

    private ConcurrentHashMap<String, DBObject> db = new ConcurrentHashMap<>();
    private TTLEstimator estimator;
    private CacheSimulator cache;
    private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

    public DBSimulator(TTLEstimator estimator) {
        this.estimator = estimator;
    }

    @Override
    public DBObject read(String key) {
        DBObject obj = db.get(key);

        if (obj == null) {
            obj = new DBObject(key, 0);
        }
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

            executorService.schedule(
                   () -> cache.purge(obj.getKey()), 0,
                    TimeUnit.MILLISECONDS);

            Thread.sleep(DistributionService.getDBWriteSample()
                    + DistributionService.getCacheToDBSample());


        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void registerCache(CacheSimulator cacheSimulator) {
        cache = cacheSimulator;
    }
}
