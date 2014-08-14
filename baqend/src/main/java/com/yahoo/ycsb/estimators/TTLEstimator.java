package com.yahoo.ycsb.estimators;

/**
 * Created by Michael on 13.08.2014.
 */
public interface TTLEstimator {

    public void registerWrite(String key);

    public long registerRead(String key);

    public long estimate();
}
