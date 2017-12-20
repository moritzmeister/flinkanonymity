package org.flinkanonymity.datatypes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class Generalization {

    public String attr;
    public String dataFilePath;
    public int level;
    private HashMap<String, String> hierarchy = new HashMap<String, String>();

    public Generalization(){
        throw new RuntimeException("Attribute needs to be specified for generalization");
    }

    public Generalization(String attribute){
        this.attr = attribute;
    }

    public Generalization(String attribute, String filepath, int hierLevel){
        this.attr = attribute;
        this.dataFilePath = filepath;
        this.level = hierLevel;

        // initialize parameters and empty HashMap
        String line = "";
        String cvsSplitBy = ",";

        //read file and initialize the generalization hierarchy in a HashMap
        try (BufferedReader br = new BufferedReader(new FileReader(this.dataFilePath))) {

            while ((line = br.readLine()) != null) {

                // use comma as separator
                String[] keyValue = line.split(cvsSplitBy);

                this.hierarchy.put(keyValue[0], keyValue[this.level]);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getHierarchy(String key){
        return this.hierarchy.get(key);
    }

    public void putHierarchy(String key, String val){
        this.hierarchy.put(key, val);
    }

}
