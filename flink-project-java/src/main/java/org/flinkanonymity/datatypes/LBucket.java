package org.flinkanonymity.datatypes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class LBucket extends Bucket{
    private String attr;
    private HashMap<String, Bucket> buckets;
    private String sensitive = "sensitive_class";

    public LBucket(String attr){
        super();
        this.attr = attr;
        this.buckets = new HashMap<String, Bucket>(); // Creating LBuckets.
    }

    public LBucket(String attr, String sensitive){
        super();
        this.attr = attr;
        this.buckets = new HashMap<String, Bucket>(); // Creating LBuckets.
        this.sensitive = sensitive;
    }

    @Override
    public void add(AdultData ad) {
        String sensitiveData = ad.getAttribute(this.sensitive);
        if (!buckets.containsKey(sensitiveData)){ // If a bucket for this sensitive data does not exists
            Bucket tempBucket = new Bucket(id = sensitiveData); // Create bucket
            tempBucket.add(ad); // Add the data
            buckets.put(sensitiveData, tempBucket); // Create this bucket. Id is just for debugging.
        } else {
            buckets.get(sensitiveData).add(ad); // Get the proper bucket for the sensitive data, // Add the data
        }


        if (buckets.get(sensitiveData).size() == 0){
            throw new RuntimeException("Created empty bucket with sensitiveData " + sensitiveData);
        }

        this.bufferSize++;
    }

    public String toString(){
        int empty = 0;
        StringBuilder sb = new StringBuilder();
        sb.append("\n!!Start! Bucket containing : " + this.buckets.size()+" buckets, in total " + this.bufferSize + " tuples. \n");
        for(Map.Entry<String, Bucket> entry : buckets.entrySet()) { // For each bucket
            Bucket b = entry.getValue();
            if (b.size() == 0){
                empty++;
            }
            sb.append(b);
        }
        sb.append("\n"+empty+" are empty.");
        return sb.toString();
    }

    public boolean isLDiverse(int l) {
        // Is the bucket l-diverse.
        return (this.buckets.size() >= l); // Are there L or more buckets? i.e. are there L or more different sensitive values

    }

    public int size(){
        // Returning size (number of tuples)
        return this.bufferSize;
    }

    public int numberOfBuckets(){
        return this.buckets.size();
    }


    public AdultData[] dropBuffer(){
        // Returns everything in the buffer and clears it.
        AdultData[] out = new AdultData[bufferSize];
        AdultData[] temp;
        int i = 0;
        for(Map.Entry<String, Bucket> entry : buckets.entrySet()) { // For each bucket
            Bucket b = entry.getValue();
            temp = b.dropBuffer();
            for (AdultData ad : temp){
                out[i] = ad;
                i ++;
            }
        }
        this.buckets = new HashMap<String, Bucket>(); // Creating LBuckets.
        this.bufferSize = 0;
        return out;
    }

}
