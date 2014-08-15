package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.estimators.StaticEstimator;
import org.apache.commons.math3.distribution.ExponentialDistribution;

import java.util.HashMap;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Michael on 12.08.2014.
 */
public class MonteCarloClient extends DB {

    private static volatile CacheSimulator cache;
    private static AtomicInteger cntr = new AtomicInteger();

    @Override
    public void init() throws DBException {
        synchronized (MonteCarloClient.class) {
            if (cache == null) {
                DistributionService d = new DistributionService(new ExponentialDistribution(10),
                        new ExponentialDistribution(10), new ExponentialDistribution(50),
                        new ExponentialDistribution(10));
                cache = new CacheSimulator(new DBSimulator(new StaticEstimator()));
            }
        }
    }

    /**
     * Cleanup any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @Override
    public void cleanup() throws DBException {
           if (cntr.incrementAndGet() == Long.parseLong(getProperties().getProperty("threadcount", "16"))) {
               System.out.println(Thread.currentThread() + "stale reads = " + StalenessDetector.countStaleReads());
               cache.printStatistics();
           }
    }

    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        try {
            long t = StalenessDetector.generateVersion();
            Thread.sleep(DistributionService.getClientToCacheSample());
            DBObject obj = cache.read(key);
            StalenessDetector.testForStaleness(key, obj.getTimeStamp(), t);

            return 0;
        } catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
    }

    @Override
    public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        return 0;
    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        try {
            Long t = StalenessDetector.generateVersion();
            DBObject obj = new DBObject(key, t);
            Thread.sleep(DistributionService.getClientToCacheSample());
            cache.write(obj);
            StalenessDetector.addVersion(key, t);
            StalenessDetector.addWriteAcknowledgement(key, StalenessDetector.generateVersion());

            return 0;
        } catch (Exception e) {
            e.printStackTrace();
            return 1;
        }

    }

    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        try {
            Long t = StalenessDetector.generateVersion();
            DBObject obj = new DBObject(key, t);
            Thread.sleep(DistributionService.getClientToCacheSample());
            cache.write(obj);
            StalenessDetector.addWriteAcknowledgement(key, StalenessDetector.generateVersion());
            StalenessDetector.addVersion(key, t);

            return 0;
        } catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
    }

    @Override
    public int delete(String table, String key) {
        return 0;
    }
}
