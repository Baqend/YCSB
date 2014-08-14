package com.yahoo.ycsb.estimators;

/**
 * Created by Michael on 13.08.2014.
 */
public class StaticEstimator implements TTLEstimator {

    public long registerRead(String key) {
        //register key somewhere

        return estimate();
    }

    public void registerWrite(String key) {
        //register write on key
    }

    @Override
    public long estimate() {
        // static 10 seconds
        return 10000;
    }
}
