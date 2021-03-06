package com.yahoo.ycsb.db;

import org.apache.commons.math3.distribution.RealDistribution;

/**
 * Created by Michael Schaarschmidt
 * <p>
 * Contains all distributions necessary for the simulation and
 * Provides samples for all distributions.
 */
public class DistributionService {

    private static RealDistribution clientToCacheDistribution;
    private static RealDistribution cacheToDBDistribution;
    private static RealDistribution clientToDBDistribution;
    private static RealDistribution purgeDistribution;
    private static RealDistribution dbWriteDistribution;

    public static int getScalingFactor() {
        return scalingFactor;
    }

    private static int scalingFactor;

    /**
     * @param toScale the timespan to scale
     * @return the scaled time
     */
    public static double scale(double toScale) {
        return toScale / scalingFactor;
    }

    public DistributionService(RealDistribution clientToCache,
                               RealDistribution cacheToDB,
                               RealDistribution purge,
                               RealDistribution dbWrite,
                               RealDistribution clientToDB,
                               int scaling) {
        clientToCacheDistribution = clientToCache;
        cacheToDBDistribution = cacheToDB;
        purgeDistribution = purge;
        dbWriteDistribution = dbWrite;
        scalingFactor = scaling;
        clientToDBDistribution = clientToDB;
    }

    public static long getClientToCacheSample() {
        return new Double(clientToCacheDistribution.sample()).longValue() / scalingFactor;
    }

    public static long getCacheToDBSample() {
        return new Double(cacheToDBDistribution.sample()).longValue() / scalingFactor;
    }

    public static long getPurgeSample() {
        return new Double(purgeDistribution.sample()).longValue() / scalingFactor;
    }

    public static long getDBWriteSample() {
        return new Double(dbWriteDistribution.sample()).longValue() / scalingFactor;
    }

    public static long getClientToDBSample() {
        return new Double(clientToDBDistribution.sample()).longValue() / scalingFactor;
    }
}
