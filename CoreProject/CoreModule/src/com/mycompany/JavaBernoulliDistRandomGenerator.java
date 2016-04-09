package com.mycompany;

import java.util.Random;

public class JavaBernoulliDistRandomGenerator extends RandomGenerator {

    private Random rd = null;
    private double prob = 0.000001;
    private double currRandomValue = 0;
    private long seed = 0;

    public JavaBernoulliDistRandomGenerator(long lnSeed, double probIn){
        this.rd = new Random(lnSeed);
        this.prob = probIn;
        this.seed = lnSeed;
    }

    public long getRandomNumber() throws Exception {
        currRandomValue = rd.nextDouble();
        return (this.currRandomValue <= this.prob ? 1 : 0);
    }

    public String getSeedIdentity(){
        return "ber:" + Long.toString(seed) + ":" + Double.toString(prob);
    };
}
