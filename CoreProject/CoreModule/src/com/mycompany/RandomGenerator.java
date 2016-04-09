package com.mycompany;

import java.io.FileNotFoundException;

public abstract class RandomGenerator {
    public abstract long getRandomNumber() throws Exception;
    public abstract String getSeedIdentity();
    public static RandomGenerator getRandomGenerator(String input) throws FileNotFoundException{
        //boolean blIsNumber = true;
        //long currRandomValue = 0;

        /*
        try{
            currRandomValue = Long.parseLong(input);
        }
        catch (Exception ex){
            blIsNumber = false;
        }
        */

        if (input.startsWith("uni:"))
            return new JavaBuiltInRandomGenerator(Long.parseLong(input.split(":")[1]));

        if (input.startsWith("geo:")){
            if (input.split(":").length == 3)
                return new JavaGeoDistRandomGenerator(Long.parseLong(input.split(":")[1]), Double.parseDouble(input.split(":")[2]));

            return new JavaGeoDistRandomGenerator(Long.parseLong(input.split(":")[1]));
        }

        if (input.startsWith("ber:"))   /// ber:38672683:0.635
            return  new JavaBernoulliDistRandomGenerator(Long.parseLong(input.split(":")[1]), Double.parseDouble(input.split(":")[2]));

        return new FileRandomGenerator(input);
    }
}
