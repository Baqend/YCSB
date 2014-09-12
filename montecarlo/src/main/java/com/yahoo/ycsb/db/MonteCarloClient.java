package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.GradientService;
import com.yahoo.ycsb.estimators.QuantileEstimator;
import com.yahoo.ycsb.estimators.SlidingWindow;
import com.yahoo.ycsb.estimators.StaticEstimator;
import org.apache.commons.math3.distribution.NormalDistribution;

import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Michael Schaarschmidt. Contact: mi.schaarschmidt@gmail.com
 * <p>
 * Implements a YCSB binding for a concurrent Monte-Carlo simulation of
 * caching behaviour that can analyse staleness and caching behaviour. To this end, multiple implementations of
 * the SimulationLayer interface are stacked into each other, thus simulating different caching layers, .i.e.,
 * CDNs, reverse-proxy caches, client caches and so forth.
 * The ultimate goal of this implementation is to help understand how different caching
 * parameters (most importantly expiration (TTL)) impact the cache-hit ratio and YCSB-throughput.
 * <p>
 * Latency round-trips are simulated through arbitrary distributions. Each operation draws a latency from the
 * appropriate distribution, assuming symmetrical behaviour (i.e. latency client-> cache == cache->client).
 * <p>
 * For a starting point, we have found Cauchy/Gamma-distributions to be reasonable approximations for latency
 * behaviour in EC2. Different implementations of the TTLEstimator interface can
 * be used in the DBSimulator to generate object-level dynamic TTLs and evaluate their impact.
 */
public class MonteCarloClient extends DB {

    private static volatile CacheLayer cache;
    private static AtomicInteger threadCount = new AtomicInteger();

    @Override
    public void init() throws DBException {
        synchronized (MonteCarloClient.class) {
            if (cache == null) {
                Properties props = getProperties();
                int scaling = Integer.parseInt(props.getProperty("scaling", "10"));

                DistributionService d = new DistributionService(new NormalDistribution(3.99/2.0, 0.11/2.0),
                        new NormalDistribution(173.04/2.0, 0.11/2.0), new NormalDistribution(100,10),
                        new NormalDistribution(10, 2), new NormalDistribution(163.89/2.0, 0.18/2.0), scaling);
/*
                cache = new CacheSimulator(new DBSimulator(
                        new QuantileEstimator(
                                new SlidingWindow(300),
                                GradientService.getMaxTtl(),
                                0.022743096825052267,
                                QuantileEstimator.RatingFunction.linear
                                )));
*/
                  cache = new CacheSimulator(new DBSimulator(
                        new StaticEstimator(1000000)));
            }
        }
    }

    /**
     * Cleanup any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @Override
    public void cleanup() throws DBException {
        // The last thread collects statistics.
        if (threadCount.incrementAndGet() == Long.parseLong(getProperties().getProperty("threadcount", "10"))) {
            cache.printStatistics(getProperties().getProperty("cacheexport", "result.txt"));
            GradientService.registerScore(cache.calculateScore());
            threadCount.set(0);
            cache = null;
            StalenessDetector.reset();
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
        // Currently not relevant for simulation purposes.
        return 0;
    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        try {
            Long t = StalenessDetector.generateVersion();
            DBObject obj = new DBObject(key, t);
            Thread.sleep(DistributionService.getClientToDBSample());
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
            Thread.sleep(DistributionService.getClientToDBSample());
            cache.write(obj);
            StalenessDetector.addVersion(key, t);
            // Note that we generate a new version here to know when the version we have just
            // written has been acknowledged.
            StalenessDetector.addWriteAcknowledgement(key, StalenessDetector.generateVersion());

            return 0;
        } catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
    }

    @Override
    public int delete(String table, String key) {
        // Currently not relevant for simulation purposes.
        return 0;
    }
}
