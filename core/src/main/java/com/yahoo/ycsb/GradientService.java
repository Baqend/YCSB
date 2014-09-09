package com.yahoo.ycsb;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;

/**
 * Created by Michael on 01.09.2014.
 */
public class GradientService {

    private static double maxTtl = 25;
    private static double intraTtl;
    private static double intraSlope;
    private static double slope = 0.1;
    private static double slopeStep;
    private static boolean done = false;
    private static int superSteps = 0;
    private static BufferedWriter writer;
    private static HashMap<Integer, Double> currentSuperStepScore = new HashMap<>();
    private static int currentIt;
    private static double currentBest;
    private static LinkedList<DescentResult> descentResults = new LinkedList<>();


    public static void init(double scale, double ttlSteps, double ttl, double initSlope, BufferedWriter resultWriter) throws IOException {
        maxTtl = ttl;
        slope = initSlope;
        slopeStep = slope * scale;
        writer = resultWriter;
        currentIt = 0;
        currentBest = Long.MAX_VALUE;
        superSteps = 0;

    }

    public static void registerScore(SimulationResult score) {
        double sc = (0.2 * score.getCacheMisses() +  0.8 * score.getInvalidations()) / 100000.0;

        try {
            writer.write("score = " + sc + " hits= " + score.getHits() + ", misses= " + score.getCacheMisses()
                    + ", invalidations= " + score.getInvalidations() + ", stale reads= " + score.getStaleReads()
                    + ", fpp= " + score.getP() + "\n");
            System.out.print("score = " + sc + " hits= " + score.getHits() + ", misses= " + score.getCacheMisses()
                    + ", invalidations= " + score.getInvalidations() + ", stale reads= " + score.getStaleReads()
                    + ", fpp= " + score.getP() + "\n");


        } catch (IOException e) {
            e.printStackTrace();
        }
        //System.out.println("Tried score = " + sc + ", slope = " + intraSlope);
        currentSuperStepScore.put(currentIt, sc);
    }

    public static double getSlope() {
        return intraSlope;
    }

    public static double getMaxTtl() {
        return intraTtl;
    }

    public static boolean finished() {
        return done;
    }

    public static void iterate(int i) {
        intraSlope = slope;
        intraTtl = maxTtl;

        currentIt = i;

        switch (i) {
            case 0:
                intraSlope += slopeStep;
                break;
            case 1:
                if (intraSlope - slopeStep > 0) {
                    intraSlope -= slopeStep;
                }
                else {
                    intraSlope = 0.0;
                }
                break;
        }
    }

    public static LinkedList<DescentResult> getDescentResults() {
        return descentResults;
    }

    public static void iterateSuperstep() {
        superSteps++;

        int k = -1;
        double current = Double.MAX_VALUE;
        for (int i = 0; i < 2; i++) {
            if (currentSuperStepScore.get(i) < current) {
                current = currentSuperStepScore.get(i);
                k = i;
            }
        }

        if (current < currentBest) {
            switch (k) {
                case 0:
                    slope += slopeStep;
                    System.out.println("increasing slope");
                    break;
                case 1:
                    if (slope - slopeStep > 0) {
                        slope -= slopeStep;
                        System.out.println("decreasing slope");
                    }
                    break;
            }

            currentBest = current;

            try {
                System.out.print("adjusted score= " + currentBest + ", slope = " + slope + "\n");
                writer.write("adjusted score= " + currentBest + ", slope = " + slope + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            done = true;
            descentResults.add(new DescentResult(currentBest, maxTtl, slope));
        }

        currentSuperStepScore.clear();
        currentIt = 0;
        if (superSteps == 50) {
            done = true;
        }
    }

    public static void reset() {
        done = false;
        currentSuperStepScore.clear();
    }

    public static void printCurrent() {
        System.out.println("maxttl = " + maxTtl);
        System.out.println("slope = " + slope);
    }
}
