package com.yahoo.ycsb.db;

/**
 * Created by Michael on 13.08.2014.
 */
public interface SimulationLayer {

    public DBObject read(String key);

    public void write(DBObject obj);


}
