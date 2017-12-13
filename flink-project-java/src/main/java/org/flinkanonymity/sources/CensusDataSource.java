package org.flinkanonymity.sources;

// Custom classes
import  org.flinkanonymity.datatypes.CensusData;


// Flink
import org.apache.flink.streaming.api.functions.source.SourceFunction;

// Java
import java.io.BufferedReader;


public class CensusDataSource implements SourceFunction<CensusData> {

    private final String dataFilePath;

    private transient BufferedReader reader;

    public CensusDataSource(String dataFilePath) {
        this.dataFilePath = dataFilePath;
    }

    public void run(SourceContext<CensusData> sourceContext) throws Exception {

    }

}
