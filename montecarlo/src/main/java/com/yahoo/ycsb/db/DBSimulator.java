package com.yahoo.ycsb.db;

import com.yahoo.ycsb.estimators.TTLEstimator;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Created by Michael on 12.08.2014.
 */
public class DBSimulator implements SimulationLayer {

    private volatile  ConcurrentHashMap<String, DBObject> db = new ConcurrentHashMap<>();
    private volatile TTLEstimator estimator;
    private volatile CacheSimulator cache;
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
            cache.purge(obj.getKey());
          //  executorService.schedule(
           //        () -> cache.purge(obj.getKey()), 0, // DistributionService.getPurgeSample(),
          //          TimeUnit.MILLISECONDS);

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
