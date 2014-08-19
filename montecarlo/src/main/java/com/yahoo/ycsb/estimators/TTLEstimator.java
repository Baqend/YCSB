package com.yahoo.ycsb.estimators;

/**
 * Created by Michael Schaarschmidt
 */
public interface TTLEstimator {

    /**
     * Keeps track of incoming writes for
     * TTL estimation purposes.
     * @param key
     */
    public void registerWrite(String key);

    /**
     * Keeps track of incoming reads and
     * returns a TTL estimation for the
     * key.
     * @param key
     * @return
     */
    public long registerRead(String key);

    /**
     * returns an estimate for a key.
     * @return
     */
    public long estimate(String key);
}
