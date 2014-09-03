package com.yahoo.ycsb;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

/**
 * Created by Michael on 01.09.2014.
 */
public class GradientService {

    private static int maxTtl = 50;
    private static int intraTtl = 50;
    private static double intraSlope = 0.1;
    private static double ttlStep;
    private static double slope = 0.1;
    private static double slopeStep;
    private static boolean done = false;
    private static int superSteps = 0;
    private static BufferedWriter writer;
    private static HashMap<Integer, Double> currentSuperStepScore = new HashMap<>();
    private static int currentIt = 0;
    private static double currentBest = Long.MAX_VALUE;

    public static void initSteps(double scale) throws IOException {
        ttlStep = maxTtl * scale;
        slopeStep = slope * scale;
        writer = new BufferedWriter(new FileWriter("result.txt", true));

    }

    public static void registerScore(SimulationResult score)  {
        double sc = (score.getCacheMisses() +  score.getInvalidations()
                +  score.getStaleReads())/ 100000.0;
        try {
            writer.write("");

        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("score = " + sc);
        currentSuperStepScore.put(currentIt, sc);
    }

    public static double getSlope() {
        return intraSlope;
    }

    public static int getMaxTtl() {
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
                intraTtl += ttlStep;
                break;
            case 1:
                intraTtl -= ttlStep;
                break;
            case 2:
                intraSlope += slopeStep;
                break;
            case 3:
                intraSlope -= slopeStep;
                break;
        }
    }

    private static void anneal() {
            ttlStep -= ttlStep * 1.10;
            slopeStep -= slopeStep * 1.10;
    }

    public static void iterateSuperstep() {
        superSteps++;

        int k = -1;
        double current = Double.MAX_VALUE;
        for (int i = 0; i < 4; i++) {
            k = currentSuperStepScore.get(i) < current ? i : k;
            current = currentSuperStepScore.get(i);
        }

        if (current < currentBest) {
            switch (k) {
                case 0:
                    maxTtl += ttlStep;
                    System.out.println("increasing maxttl");
                    break;
                case 1:
                    maxTtl -= ttlStep;
                    System.out.println("decreasing maxttl");
                    break;
                case 2:
                    slope += slopeStep;
                    System.out.println("increasing slope");
                    break;
                case 3:
                    if (slope - slopeStep > 0) {
                        slope -= slopeStep;
                        System.out.println("decreasing slope");
                    }
                    break;
            }
            if (current - currentBest < current * 1.025) {
                anneal();
            }
            currentBest = current;

        }
        else {
            done = true;
            try {
            writer.flush();
            writer.close();
            }
            catch (Exception e) {
                e.printStackTrace();
            }

        }

        currentSuperStepScore.clear();

        if (superSteps == 50) {
            done = true;
        }
    }

    public static void printCurrent() {
        System.out.println("maxttl = " + maxTtl);
        System.out.println("slope = " + slope);
    }
}
