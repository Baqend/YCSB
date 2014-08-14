package com.yahoo.ycsb.db;

/**
 * Created by Michael on 13.08.2014.
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

    public void setKey(String key) {
        this.key = key;
    }

    public long getExpiration() {
        return expiration;
    }

    public void setExpiration(long expiration) {
        this.expiration = expiration;
    }
}
