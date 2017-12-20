package org.flinkanonymity.datatypes;

import java.lang.reflect.Array;
import java.util.LinkedList;

public class Bucket {

    private int bufferSize;
    private boolean workNode = false;
    private LinkedList<AdultData> buffer;

    public Bucket(){
        this.bufferSize = 0;
        this.buffer = new LinkedList<>();
    }

    public void add(AdultData ad) {
        buffer.add(ad);
        bufferSize++;
    }

    public boolean isKAnonymous(int k){
        return (bufferSize >= k);
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
