package com.yahoo.ycsb.db;

/**
 * Created by Michael Schaarschmidt
 * <p>
 * All parts of the simulation (except for the YCSB client binding)
 * implement this interface.
 */
public interface SimulationLayer {

    /**
     * Basic read operation. Reads may be
     * passed through an arbitrary number of simulation layers.
     *
     * @param key
     * @return
     */
    public DBObject read(String key);

    /**
     * Basic write operation. Objects may be passed through multiple
     * simulation layers (e.g. caches) until they are actually evaluated.
     *
     * @param obj
     */
    public void write(DBObject obj);


}
