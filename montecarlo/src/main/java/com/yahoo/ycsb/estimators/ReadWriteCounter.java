package com.yahoo.ycsb.estimators;

/**
 * Created by Emil on 28.08.2014.
 */
public interface ReadWriteCounter {

    void registerWrite(String key);

    void registerRead(String key);

    double getReadFrequency(String key);

    double getWriteFrequency(String key);
}
