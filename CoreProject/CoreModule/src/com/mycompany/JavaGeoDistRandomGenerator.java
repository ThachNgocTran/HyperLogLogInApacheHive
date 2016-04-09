package com.mycompany;

import java.util.Random;

public class JavaGeoDistRandomGenerator extends RandomGenerator {

    private Random rd = null;
    private double prob = 0.000001;         // with this, we can have values more than 15mil
    private double currRandomValue = 0;
    private long seed = 0;

    public JavaGeoDistRandomGenerator(long lnSeed){
        rd = new Random(lnSeed);
        seed = lnSeed;
    }

    public JavaGeoDistRandomGenerator(long lnSeed, double probability){
        this(lnSeed);
        this.prob = probability;
    }

    public long getRandomNumber() throws Exception {
        currRandomValue = rd.nextDouble();
        return (long)Math.ceil(Math.log((double) currRandomValue)/Math.log(1.0-prob));
    }

    public String getSeedIdentity(){
        return "geo:" + Long.toString(seed) + (prob != 0.000001 ? ":" + prob : "");
    };
}
