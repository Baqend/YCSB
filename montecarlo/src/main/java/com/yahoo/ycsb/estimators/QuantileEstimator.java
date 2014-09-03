package com.yahoo.ycsb.estimators;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by Emil on 28.08.2014.
 */
public class QuantileEstimator implements TTLEstimator {

    private final RatingFunction func;
    private ReadWriteCounter counter;
    private long maxTTL;
    private double slope;
    private BufferedWriter writer;


    /**
     * @param counter the counter to use for read, write frequencies
     * @param maxTTL  Maximum allowed cache expiration TTL in s
     * @param slope   the slope of the linear function. Example: 0.2 would indicate that if there are 5 times more cache misses that writes, the maxTTL will be estimated
     */
    public QuantileEstimator(ReadWriteCounter counter, long maxTTL, double slope, RatingFunction func) {
        this.counter = counter;
        this.maxTTL = maxTTL * 1000000000l;
        this.slope = slope;
        this.func = func;
        try {
            writer =  new BufferedWriter(new FileWriter("ttls.txt", true));
        } catch (IOException e) {
            e.printStackTrace();
        }
        ;
    }

    @Override
    public void registerWrite(String key) {
        counter.registerWrite(key);
    }

    @Override
    public long registerRead(String key) {
        counter.registerRead(key);
        return estimate(key);
    }

    @Override
    public long estimate(String key) {
        Double w = counter.getWriteFrequency(key);
        Double r = counter.getReadFrequency(key);

        if (w == null ||r == null) {
            return maxTTL;
        }


        double upperProb = PoissonCalculator.writeCDF(w, maxTTL);

        double ratio = PoissonCalculator.ratio(r, w);
        double targetProbability = getTargetProbability(ratio, upperProb);
        long ttl;
        if (targetProbability <= 0) {
            ttl = 0;
        } else if (targetProbability >= upperProb) {
            ttl = maxTTL;
        } else {
            double quantile = PoissonCalculator.writeQuantile(w, targetProbability);
            ttl = (long) Math.floor(quantile);
        }
//        try {
//            writer.write("readFrequency= " + r + ", writeFrequency = " + w + ",ttl = " + ttl + "\n");
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        return ttl;
    }

    private double getTargetProbability(double ratio, double upperProb) {
        if (func == RatingFunction.linear) {
            return 0.5 + slope * ratio;
        } else if (func == RatingFunction.logistic) {
            return upperProb / (upperProb / 0.5) * Math.pow(Math.E, -slope * ratio);
        } else {
            return 0;
        }
    }

    public static enum RatingFunction {
        linear,
        logistic,
        polynomial
    }

    public long estimateFromRates(Double r, Double w) {
        double upperProb = PoissonCalculator.writeCDF(w, maxTTL);

        double ratio = PoissonCalculator.ratio(r, w);
        double targetProbability = getTargetProbability(ratio, upperProb);
        long ttl;
        if (targetProbability <= 0) {
            ttl = 0;
        } else if (targetProbability >= upperProb) {
            ttl = maxTTL;
        } else {
            double quantile = PoissonCalculator.writeQuantile(w, targetProbability);
            ttl = (long) Math.floor(quantile);
        }

        return ttl;
    }
}
