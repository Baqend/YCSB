package com.yahoo.ycsb.estimators;

/**
 * Created by Emil on 28.08.2014.
 */
public interface ReadWriteCounter {

    void registerWrite(String key);

    void registerRead(String key);

    Double getReadFrequency(String key);

    Double getWriteFrequency(String key);
}
