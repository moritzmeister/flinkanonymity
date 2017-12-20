package org.flinkanonymity.jobs;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.flinkanonymity.datatypes.Bucket;
import org.flinkanonymity.datatypes.CensusData;
import org.flinkanonymity.sources.CensusDataSource;

import java.util.HashMap;

public class Job {
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        // final String filePath = params.getRequired("input");

        String dataFilePath = "../sample-data/ipums_usa/usa_00001_sample.csv";

        // Set up Hashmap
        HashMap<CensusData, Bucket> hashMap = new HashMap<>();

        // Setup variables
        CensusData tuple;
        Bucket b;
        CensusData[] tuples;
        int k = 4;

        // Setting up Environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<CensusData> data = env.addSource(new CensusDataSource(dataFilePath));

        // While true

        // Read new tuple

        // Generalize Quasi Identifiers

        // Get bucket through hashmap
        b = hashMap(tuple);

        // If bucket is worknode
        if (b.isWorkNode()) {
            // output tuple
            b = b;
        } else {
            // else bucket.add()
            b.add(tuple);
            if (b.isKAnonymous(k)){ // if bucket satisfies k-anonymity
                // set bucket as worknode
                b.markAsWorkNode();

                // get tuples and drop bucket
                tuples = b.dropBuffer();

                // output tuples
            }
        }





        data.print();

        env.execute();
    }
}
