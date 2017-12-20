package org.flinkanonymity.jobs;

import org.apache.flink.api.java.operators.translation.PlanFilterOperator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
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

        // Set file paths
        String dataFilePath = "../sample-data/arx_adult/adult_subset.csv";
        String sex_hierarchy = "../sample-data/arx_adult/adult_hierarchy_sex.csv";
        String age_hierarchy = "../sample-data/arx_adult/adult_hierarchy_age.csv";
        String race_hierarchy = "../sample-data/arx_adult/adult_hierarchy_race.csv";
        // String marst_hierarchy = "../sample-data/arx_adult/adult_hierarchy_marital-status.csv";
        // String educ_hierarchy = "../sample-data/arx_adult/adult_hierarchy_education.csv";
        // String country_hierarchy = "../sample-data/arx_adult/adult_hierarchy_native-country.csv";
        // String workclass_hierarchy = "../sample-data/arx_adult/adult_hierarchy_workclass.csv";
        // String occ_hierarchy = "../sample-data/arx_adult/adult_hierarchy_occupation.csv";
        // String salary_hierarchy = "../sample-data/arx_adult/adult_hierarchy_salary-class.csv";

        // Initialize generalizations
        Generalization age = new Generalization("age", age_hierarchy, 1);
        Generalization sex = new Generalization("sex", sex_hierarchy, 1);
        Generalization race = new Generalization("race", race_hierarchy,1);

        // Initialize QuasiIdentifier
        QuasiIdentifier QID = new QuasiIdentifier(age, sex, race);

        // Set up Hashmap - has to be final in order to be used in HashMapFunction later on.
        final HashMap<AdultData, Bucket> hashMap = new HashMap<>();

        // Define k in k-anonymity
        final int k = 4;

        // Setting up Environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<AdultData> data = env.addSource(new AdultDataSource(dataFilePath));

        // DataStreamSink<AdultData> output = new DataStreamSink<AdultData>();
        // output.setParallelism(1);


        // Generalize Quasi Identifiers
        DataStream<AdultData> genData = data; //.map(asdasda);

        DataStream<AdultData> output = genData.flatMap(new FlatMapFunction<AdultData, AdultData>() {
            @Override
            public void flatMap(AdultData tuple, Collector<AdultData> out) throws Exception {
                // get bucket
                Bucket b = hashMap.get(tuple);
                if (b.isWorkNode()) {
                    // output tuple
                    out.collect(tuple);
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

        output.print();

        env.execute();
    }
}