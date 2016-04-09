package com.mycompany;

import java.util.Random;

public class JavaBuiltInRandomGenerator extends RandomGenerator {
    private Random rd = null;
    private long seed = 0;

    public JavaBuiltInRandomGenerator(long lnSeed){
        rd = new Random(lnSeed);
        seed = lnSeed;
    }

    public long getRandomNumber(){
        return rd.nextLong();
    };

    public String getSeedIdentity(){
        return "uni:" + Long.toString(seed);
    };
}
