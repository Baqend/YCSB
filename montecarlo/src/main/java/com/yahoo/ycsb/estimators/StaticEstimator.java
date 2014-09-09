package com.yahoo.ycsb.estimators;

import com.yahoo.ycsb.db.DistributionService;

/**
 * Created by Michael Schaarschmidt
 */
public class StaticEstimator implements TTLEstimator {

    private int scale = DistributionService.getScalingFactor();
    private long ttl;

    public StaticEstimator(long ttl) {
        this.ttl = ttl * 1000_000_000l;
    }

    public long registerRead(String key) {
        //register read somewhere

        return estimate(key);
    }


    public void registerWrite(String key) {
        //register write on key
    }

    @Override
    public long estimate(String key) {
        // static 10 seconds
        return ttl;
    }
}
