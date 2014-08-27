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
    private static RealDistribution purgeDistribution;
    private static RealDistribution dbWriteDistribution;

    public static int getScalingFactor() {
        return scalingFactor;
    }

    private static int scalingFactor;

    public DistributionService(RealDistribution clientToCache,
                               RealDistribution cacheToDB,
                               RealDistribution purge,
                               RealDistribution dbWrite, int scaling) {
        clientToCacheDistribution = clientToCache;
        cacheToDBDistribution = cacheToDB;
        purgeDistribution = purge;
        dbWriteDistribution = dbWrite;
        scalingFactor = scaling;
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
}
