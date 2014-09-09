package com.yahoo.ycsb;

/**
 * Created by Administrator on 04.09.2014.
 */
public class DescentResult {
    private double score;
    private double ttl;
    private double slope;

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    public double getTtl() {
        return ttl;
    }

    public void setTtl(double ttl) {
        this.ttl = ttl;
    }

    public double getSlope() {
        return slope;
    }

    public void setSlope(double slope) {
        this.slope = slope;
    }

    public DescentResult(double score, double ttl, double slope) {

        this.score = score;
        this.ttl = ttl;
        this.slope = slope;
    }
}
