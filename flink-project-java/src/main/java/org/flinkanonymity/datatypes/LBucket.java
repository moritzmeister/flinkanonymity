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
            buckets.put(sensitiveData, new Bucket()); // Create this bucket
        }
        Bucket b = buckets.get(sensitiveData); // Get the proper bucket for the sensitive data.
        b.add(ad); // Add the data
        this.bufferSize++;
    }

    public boolean isLDiverse(int l) {
        // Is the bucket l-diverse.
        return (buckets.size() >= l); // Are there L or more buckets? i.e. are there L or more different sensitive values
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
        this.bufferSize = 0;
        return out;
    }

}
