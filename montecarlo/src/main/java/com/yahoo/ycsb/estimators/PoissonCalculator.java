package com.yahoo.ycsb.estimators;

/**
 * Performs calculations for Poisson Processes of writes and cache misses
 */
public class PoissonCalculator {

    public static double ratio(double r, double w) {
        if (r >= w) {
            return r / w - 1;
        } else {
            return -(w / r - 1);
        }
    }

    public static double writeQuantile(double w, double p) {
        return - Math.log(1 - p) / w;
    }

    public static double writeCDF(double w, double timespan) {
        if (timespan < 0) {
            return 0;
        } else {
            return 1 - Math.pow(Math.E, -w * timespan);
        }
    }
}
