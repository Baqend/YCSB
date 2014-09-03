package com.yahoo.ycsb;

/**
 * Created by Michael on 01.09.2014.
 */
public class SimulationResult {
    private long cacheMisses;
    private long invalidations;

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

    public SimulationResult(long cacheMisses, long invalidations, long staleReads) {
        this.cacheMisses = cacheMisses;
        this.invalidations = invalidations;

        this.staleReads = staleReads;
    }

    private long staleReads;

}
