package com.yahoo.ycsb.db;

/**
 * Created by Michael Schaarschmidt
 *
 * This class implements a bean object for simulation purposes.
 * It contains only fields that are necessary for the simulation, no
 * actual values.
 */
public class DBObject {

    private volatile String key;
    private volatile long timeStamp;
    private volatile long expiration;

    public long getTimeStamp() {
        return timeStamp;
    }

    public DBObject(String key, long timeStamp) {
        this.key = key;
        this.timeStamp = timeStamp;
    }

    public DBObject(String key) {
        this.key = key;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getKey() {
        return key;
    }

    public long getExpiration() {
        return expiration;
    }

    public void setExpiration(long expiration) {
        this.expiration = expiration;
    }


}
