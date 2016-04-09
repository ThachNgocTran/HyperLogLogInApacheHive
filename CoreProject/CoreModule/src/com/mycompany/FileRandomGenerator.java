package com.mycompany;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;

public class FileRandomGenerator extends RandomGenerator {

    private BufferedReader br = null;   // don't know where to close the file...
    private String fileName = "";

    public FileRandomGenerator(String fileName) throws FileNotFoundException{
        br = new BufferedReader(new FileReader(fileName));
        this.fileName = fileName;
    }

    public long getRandomNumber() throws Exception{
        String line = br.readLine();        // a lot of garbage here.
        if (line == null)
            throw new Exception("End of file");

        return Long.parseLong(line);
    }

    public String getSeedIdentity(){
        return fileName;
    };
}
