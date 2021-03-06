package org.flinkanonymity.datatypes;

import java.lang.reflect.Array;
import java.util.LinkedList;

public class Bucket {

    int bufferSize;
    String id = "";
    boolean workNode = false;
    LinkedList<AdultData> buffer;

    public Bucket(){
        this.bufferSize = 0;
        this.buffer = new LinkedList<>();
    }
    public Bucket(String id){
        this.bufferSize = 0;
        this.buffer = new LinkedList<>();
        this.id = id;
    }

    public void add(AdultData ad) {
        buffer.add(ad);
        bufferSize++;
    }

    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("Bucket (" + this.id + "):");
        for (AdultData ad : buffer) {
            sb.append(ad).append("\n");
        }
        sb.append("\n");
        return sb.toString();
    }

    public boolean isKAnonymous(int k){
        return (bufferSize >= k);
    }

    public int size(){
        return this.buffer.size();
    }

    public void markAsWorkNode() {
        this.workNode = true;
    }

    public boolean isWorkNode() {
        return workNode;
    }
    public AdultData[] dropBuffer(){
        // Returns everything in the buffer and clears it.
        AdultData[] out = {};
        this.bufferSize = 0;
        out = buffer.toArray(out);
        buffer.clear();
        return out;
    }
}
