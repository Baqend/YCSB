package com.yahoo.ycsb.estimators;

import com.yahoo.ycsb.db.DistributionService;

/**
 * Created by Michael Schaarschmidt
 */
public class StaticEstimator implements TTLEstimator {

    private int scale = DistributionService.getScalingFactor();

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
        return 10_000_000_000l / scale;
    }
}
