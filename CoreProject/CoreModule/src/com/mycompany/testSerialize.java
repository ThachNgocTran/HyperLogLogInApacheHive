package com.mycompany;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;

import java.io.*;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class testSerialize {

    private static boolean useCompression = false;

    public static void toFile(String fileName, Serializable obj) throws Exception{
        // http://www.javapractices.com/topic/TopicAction.do?Id=57
        OutputStream file = new FileOutputStream(fileName);
        OutputStream buffer = new BufferedOutputStream(file);
        ObjectOutput output = new ObjectOutputStream(buffer);
        output.writeObject(obj);
        output.close();
    }

    public static HyperLogLogPlus fromFile(String fileName) throws Exception {
        InputStream file = new FileInputStream(fileName);
        InputStream buffer = new BufferedInputStream(file);
        ObjectInput input = new ObjectInputStream (buffer);

        HyperLogLogPlus hll = (HyperLogLogPlus)input.readObject();
        input.close();

        return hll;
    }

    /* This function uses Java Serialization to serialize the instance ==> Java's own format. */
    public static byte[] toByteArray(Serializable obj) throws Exception{
        // https://github.com/addthis/stream-lib/blob/master/src/test/java/com/clearspring/analytics/stream/cardinality/TestHyperLogLog.java
        // https://github.com/addthis/stream-lib/blob/master/src/test/java/com/clearspring/analytics/TestUtils.java

        /*
        https://github.com/addthis/stream-lib/blob/master/src/test/java/com/clearspring/analytics/stream/cardinality/TestHyperLogLogPlus.java#L179
        We can use getBytes() or serialization to save generate a byte array that represents the HLL instance and can be saved to disk.
        The difference between them are: getBytes() => use the stream-lib devs' own method of serialization; while "Serializable" uses Java's serialization.
        */

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(baos);
        out.writeObject(obj);
        out.close();

        byte[] originalData = baos.toByteArray();
        byte[] finalData = null;

        if (useCompression){
            finalData = compressData(originalData);
        }
        else{
            finalData = originalData;
        }

        return finalData;
    }

    /* This function uses Java Serialization to serialize the instance ==> Java's own format. */
    public static HyperLogLogPlus fromByteArray(byte[] dataIn) throws Exception{

        byte[] finalData = null;
        if (useCompression){
            finalData = decompressData(dataIn);
        }
        else{
            finalData = dataIn;
        }

        ByteArrayInputStream bais = new ByteArrayInputStream(finalData);
        ObjectInputStream in = new ObjectInputStream(bais);

        HyperLogLogPlus hll = (HyperLogLogPlus)in.readObject();
        in.close();

        return hll;
    }

    /* This function uses HyperLogLog stream-lib Serialization to serialize the instance ==> Stream-Lib's own format. */
    public static byte[] toByteArray2(HyperLogLogPlus hllp) throws IOException {
        //byte[] originalData = hllp.getBytes();
        /*
        byte[] finalData = null;

        if (useCompression){
            finalData = compressData(originalData);
        }
        else{
            finalData = originalData;
        }

        return finalData;
        */
        return hllp.getBytes();
    }

    /* This function uses HyperLogLog stream-lib Serialization to serialize the instance ==> Stream-Lib's own format. */
    public static byte[] toByteArray2(HyperLogLogPlus hllp, boolean useCompression) throws IOException {
        byte[] originalData = toByteArray2(hllp);

        if (useCompression == false)
            return originalData;

        return compressData(originalData);
    }

    /* This function uses HyperLogLog stream-lib Serialization to serialize the instance ==> Stream-Lib's own format. */
    public static HyperLogLogPlus fromByteArray2(byte[] dataIn) throws Exception{
        /*
        byte[] finalData = null;

        if (useCompression){
            finalData = decompressData(dataIn);
        }
        else{
            finalData = dataIn;
        }
        */
        return HyperLogLogPlus.Builder.build(dataIn);
    }

    /* This function uses HyperLogLog stream-lib Serialization to serialize the instance ==> Stream-Lib's own format. */
    public static HyperLogLogPlus fromByteArray2(byte[] dataIn, boolean useCompression) throws Exception{
        byte[] data = null;

        if (useCompression == false)
            data = dataIn;
        else
            data = decompressData(dataIn);

        return HyperLogLogPlus.Builder.build(data);
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Convert Serializable Object to Byte Array and Vice Versa
    public static byte[] fromObjectToByteArray(Serializable obj) throws Exception{
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(baos);
        out.writeObject(obj);
        out.close();

        byte[] originalData = baos.toByteArray();
        byte[] finalData = null;

        if (useCompression){
            finalData = compressData(originalData);
        }
        else{
            finalData = originalData;
        }

        return finalData;
    }

    public static Object fromByteArrayToObject(byte[] dataIn) throws Exception{
        byte[] finalData = null;
        if (useCompression){
            finalData = decompressData(dataIn);
        }
        else{
            finalData = dataIn;
        }

        ByteArrayInputStream bais = new ByteArrayInputStream(finalData);
        ObjectInputStream in = new ObjectInputStream(bais);

        Object objAnything = in.readObject();   // the using function should cast this to anything it wants.
        in.close();

        return objAnything;
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // For HyperLogLogPlusAndMinHash - Use Java Serialization
    public static HyperLogLogPlusAndMinHash fromByteArrayToHllpAMh(byte[] dataIn) throws Exception{

        byte[] finalData = null;
        if (useCompression){
            finalData = decompressData(dataIn);
        }
        else{
            finalData = dataIn;
        }

        ByteArrayInputStream bais = new ByteArrayInputStream(finalData);
        ObjectInputStream in = new ObjectInputStream(bais);

        HyperLogLogPlusAndMinHash hllamh = (HyperLogLogPlusAndMinHash)in.readObject();
        in.close();

        return hllamh;
    }

    public static byte[] fromHllpAMhToByteArray(HyperLogLogPlusAndMinHash hllpamh) throws Exception{
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(baos);
        out.writeObject(hllpamh);
        out.close();

        byte[] originalData = baos.toByteArray();
        byte[] finalData = null;

        if (useCompression){
            finalData = compressData(originalData);
        }
        else{
            finalData = originalData;
        }

        return finalData;
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // COMPRESSION AND DECOMPRESSION
    public static byte[] compressData(byte[] originalData) throws IOException {
        // https://dzone.com/articles/how-compress-and-uncompress
        Deflater compressor = new Deflater(Deflater.BEST_COMPRESSION);

        compressor.setInput(originalData);
        compressor.finish();

        // Default buffer is 32, but automatically expand as necessary ==> use pre-allocated for performance.
        // In most cases, the compressed data is smaller than the original data.
        // In rare cases, the data is truly random, resulting in bigger size after compressing;
        // In this case, ByteArrayOutputStream will allocate a new buffer...
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(originalData.length);

        byte[] readBuffer = new byte[4096];
        int readCount = 0;

        while (!compressor.finished()){
            readCount = compressor.deflate(readBuffer);
            outputStream.write(readBuffer, 0, readCount);
        }

        outputStream.close();
        compressor.end();

        return outputStream.toByteArray();
    }

    public static byte[] decompressData(byte[] compressedData) throws DataFormatException, IOException{
        Inflater decompressor = new Inflater();

        decompressor.setInput(compressedData);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(compressedData.length);

        byte[] readBuffer = new byte[4096];
        int readCount = 0;

        while(!decompressor.finished()){
            readCount = decompressor.inflate(readBuffer);
            outputStream.write(readBuffer, 0,readCount);
        }

        outputStream.close();
        decompressor.end();

        return outputStream.toByteArray();
    }
}

