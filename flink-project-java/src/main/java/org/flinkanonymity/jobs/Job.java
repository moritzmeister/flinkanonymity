package org.flinkanonymity.jobs;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.FlatMapFunction;

import org.flinkanonymity.datatypes.AdultData;
import org.flinkanonymity.datatypes.Bucket;
import org.flinkanonymity.datatypes.Generalization;
import org.flinkanonymity.datatypes.QuasiIdentifier;
import org.flinkanonymity.sources.AdultDataSource;

import java.util.HashMap;

public class Job {
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        // final String filePath = params.getRequired("input");

        String dataFilePath = "../sample-data/ipums_usa/usa_00001_sample.csv";

        // Set up Hashmap
        HashMap<AdultData, Bucket> hashMap = new HashMap<>();

        // Setup variables
        int k = 4;

        // Setting up Environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<AdultData> data = env.addSource(new AdultDataSource(dataFilePath));

        // DataStreamSink<AdultData> output = new DataStreamSink<AdultData>();
        // output.setParallelism(1);


        // Generalize Quasi Identifiers
        DataStream<AdultData> genData = data.map(asdasda);

        DataStream<AdultData> output = genData.flatMap(new FlatMapFunction<AdultData, AdultData>() {
            @Override
            public void flatMap(AdultData tuple, Collector<AdultData> out) throws Exception {
                // get bucket
                Bucket b = hashMap.get(tuple);
                if (b.isWorkNode()) {
                    // output tuple
                    out.collect(b)
                } else {
                    b.add(tuple);
                    if (b.isKAnonymous(k)) { // if bucket satisfies k-anonymity
                        // set bucket as worknode
                        b.markAsWorkNode();

                        // get tuples and drop bucket
                        AdultData[] tuples = b.dropBuffer();

                        // output tuples
                        for (AdultData t : tuples) {
                            out.collect(t);
                        }
                    }
                }
            }
        });

        /*
        // If bucket is worknode

        */





        data.print();

        env.execute();
    }
}

