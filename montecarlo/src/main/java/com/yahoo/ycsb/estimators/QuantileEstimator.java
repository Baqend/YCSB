package com.yahoo.ycsb.estimators;

/**
 * Created by Emil on 28.08.2014.
 */
public class QuantileEstimator implements TTLEstimator {

    private final RatingFunction func;
    private ReadWriteCounter counter;
    private long maxTTL;
    private double slope;

    /**
     * @param counter the counter to use for read, write frequencies
     * @param maxTTL  Maximum allowed cache expiration TTL
     * @param slope   the slope of the linear function. Example: 0.2 would indicate that if there are 5 times more cache misses that writes, the maxTTL will be estimated
     */
    public QuantileEstimator(ReadWriteCounter counter, long maxTTL, double slope, RatingFunction func) {
        this.counter = counter;
        this.maxTTL = maxTTL;
        this.slope = slope;
        this.func = func;
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
        double w = counter.getWriteFrequency(key);
        double r = counter.getReadFrequency(key);

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
}
