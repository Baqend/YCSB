package com.yahoo.ycsb.db;

import com.yahoo.ycsb.SimulationResult;

/**
 * Created by Michael Schaarschmidt
 */
public interface CacheLayer extends SimulationLayer {

    /**
     * Removes an object from a cache. Any implementation of
     * this method needs to take care of concurrent reads that
     * may try to insert already purged objects into the cache.
     *
     * @param obj
     */
    public void purge(DBObject obj);

    /**
     * Prints basic cache statistics, i.e.
     * cache hit ratio and invalidations.
     */
    public void printStatistics(String fileName);

    /**
     * Calculates cache score
     *
     */
    public SimulationResult calculateScore();


}