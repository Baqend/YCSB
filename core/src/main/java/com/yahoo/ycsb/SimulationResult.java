package com.yahoo.ycsb;

/**
 * Created by Michael on 01.09.2014.
 */
public class SimulationResult {
    private long cacheMisses;
    private long invalidations;
    private long hits;
    private double p;

    public double getP() {
        return p;
    }

    public void setP(double p) {
        this.p = p;
    }

    public SimulationResult(long cacheMisses, long invalidations, long hits, double p, long staleReads) {

        this.cacheMisses = cacheMisses;
        this.invalidations = invalidations;
        this.hits = hits;
        this.p = p;
        this.staleReads = staleReads;
    }

    public long getCacheMisses() {
        return cacheMisses;
    }

    public void setCacheMisses(long cacheMisses) {
        this.cacheMisses = cacheMisses;
    }

    public long getInvalidations() {
        return invalidations;
    }

    public void setInvalidations(long invalidations) {
        this.invalidations = invalidations;
    }

    public long getStaleReads() {
        return staleReads;
    }

    public void setStaleReads(long staleReads) {
        this.staleReads = staleReads;
    }

    private long staleReads;

    public long getHits() {
        return hits;
    }

    public void setHits(long hits) {
        this.hits = hits;
    }
}
