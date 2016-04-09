package com.mycompany;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Utils {

    public static String getTimeNow(){
        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        Date now = new Date();
        String strDate = sdfDate.format(now);
        String strSessionTime = strDate.toString();

        return strSessionTime;
    }

    /*
    https://stackoverflow.com/questions/2808535/round-a-double-to-2-decimal-places
    We have built-in Java Round Function (https://docs.oracle.com/javase/7/docs/api/java/lang/Math.html#round(double)), but it rounds from DOUBLE to LONG only.
     */
    public static double round(double value, int places) {
        if (places < 0) throw new IllegalArgumentException();

        long factor = (long) Math.pow(10, places);
        value = value * factor;
        long tmp = Math.round(value);
        return (double) tmp / factor;
    }
}

