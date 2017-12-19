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

        // Setting up Environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<CensusData> data = env.addSource(new CensusDataSource(dataFilePath));

        // While true

        // Read new tuple

        // Generalize Quasi Identifiers

        // Get bucket through hashmap

        // If bucket is worknode
            // output tuple
        // else bucket.add()
            // if bucket satisfies k-anonymity
                // set bucket as worknode
                // get tuples and drop bucket

        data.print();

        env.execute();
    }
}
