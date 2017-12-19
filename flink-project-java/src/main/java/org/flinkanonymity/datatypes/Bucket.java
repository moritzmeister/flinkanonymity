package org.flinkanonymity.datatypes;

import java.lang.reflect.Array;
import java.util.LinkedList;

public class Bucket {

    private int bufferSize;
    private boolean workNode = false;
    private LinkedList<CensusData> buffer;

    public Bucket(){
        this.bufferSize = 0;
        this.buffer = new LinkedList<>();
    }

    public void add(CensusData cd) {
        buffer.add(cd);
        bufferSize++;
    }

    public boolean isKAnonymous(int k){
        return (bufferSize >= k);
    }

    public void setWorkNode() {
        this.workNode = true;
    }

    public boolean isWorkNode() {
        return workNode;
    }
    public CensusData[] dropBuffer(){
        // Returns everything in the buffer and clears it.
        CensusData[] out = {};
        out = buffer.toArray(out);
        buffer.clear();
        return out;
    }
}
