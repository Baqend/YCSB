package com.yahoo.ycsb.db;

import org.apache.commons.math3.distribution.RealDistribution;

/**
 * Created by Michael on 12.08.2014.
 */
public class DistributionService {

    private static RealDistribution clientToCacheDistribution;
    private static RealDistribution cacheToDBDistribution;
    private static RealDistribution purgeDistribution;
    private static RealDistribution dbWriteDistribution;

    public DistributionService(RealDistribution clientToCache,
                               RealDistribution cacheToDB,
                               RealDistribution purge,
                               RealDistribution dbWrite)
    {
        clientToCacheDistribution = clientToCache;
        cacheToDBDistribution = cacheToDB;
        purgeDistribution = purge;
        dbWriteDistribution = dbWrite;
    }

    public static long getClientToCacheSample() {
        return new Double(clientToCacheDistribution.sample()).longValue();
    }

    public static long getCacheToDBSample() {
        return new Double(cacheToDBDistribution.sample()).longValue();
    }

    public static long getPurgeSample() {
        return new Double(purgeDistribution.sample()).longValue();
    }

    public static long getDBWriteSample() {
        return new Double(dbWriteDistribution.sample()).longValue();
    }
}
