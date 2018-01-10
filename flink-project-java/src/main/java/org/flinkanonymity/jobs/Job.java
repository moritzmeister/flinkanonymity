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
import org.apache.flink.api.common.functions.MapFunction;


import org.flinkanonymity.datatypes.*;
import org.flinkanonymity.sources.AdultDataSource;

import java.util.HashMap;

public class Job {
    // Set up QID and Hashmap for global use.
    static QuasiIdentifier QID;
    static HashMap<String, Bucket> hashMap;
    static int k = 20;
    static int l = 5;

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        // final String filePath = params.getRequired("input");

        // Set file paths
        String dataFilePath = "../sample-data/arx_adult/adult_subset.csv";
        String sex_hierarchy = "../sample-data/arx_adult/adult_hierarchy_sex.csv";
        String age_hierarchy = "../sample-data/arx_adult/adult_hierarchy_age.csv";
        String race_hierarchy = "../sample-data/arx_adult/adult_hierarchy_race.csv";
        String marst_hierarchy = "../sample-data/arx_adult/adult_hierarchy_marital-status.csv";
        String educ_hierarchy = "../sample-data/arx_adult/adult_hierarchy_education.csv";
        String country_hierarchy = "../sample-data/arx_adult/adult_hierarchy_native-country.csv";
        // String workclass_hierarchy = "../sample-data/arx_adult/adult_hierarchy_workclass.csv";
        // String occ_hierarchy = "../sample-data/arx_adult/adult_hierarchy_occupation.csv";
        // String salary_hierarchy = "../sample-data/arx_adult/adult_hierarchy_salary-class.csv";

        // Initialize generalizations
        Generalization age = new Generalization("age", age_hierarchy, 1);
        Generalization sex = new Generalization("sex", sex_hierarchy, 0);
        Generalization race = new Generalization("race", race_hierarchy, 0);
        Generalization educ = new Generalization("educ", educ_hierarchy,2);
        Generalization marst = new Generalization("marst", marst_hierarchy,0);
        //Generalization workclass = new Generalization("workclass", workclass_hierarchy,1);
        Generalization country = new Generalization("country", country_hierarchy,1);


        // Generalization race = new Generalization("race", educ_hierarchy,1);

        // Initialize QuasiIdentifier
        QID = new QuasiIdentifier(age, sex, race, educ, marst, country);
        hashMap = new HashMap<>();


        // Setting up Environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<AdultData> data = env.addSource(new AdultDataSource(dataFilePath));

        // DataStreamSink<AdultData> output = new DataStreamSink<AdultData>();
        // output.setParallelism(1);

        // Generalize Quasi Identifiers
        DataStream<AdultData> genData = data.map(new Generalize());


        //DataStream<AdultData> output = genData.flatMap(new KAnonymize());
        DataStream<AdultData> output = genData.flatMap(new LDiversify());


        output.print();

        // genData.print();
        env.execute();
    }
    public static class Generalize implements MapFunction<AdultData, AdultData>{
        @Override
        public AdultData map(AdultData adult) throws Exception{
            return QID.generalize(adult);
        }
    }

    public static class LDiversify implements FlatMapFunction<AdultData, AdultData>{
        @Override
        public void flatMap(AdultData tuple, Collector<AdultData> out) throws Exception {
            // get bucket
            String TupleQuasiString = tuple.QuasiToString(QID);
            if (!hashMap.containsKey(TupleQuasiString)) { // If this bucket has never been reached before
                hashMap.put(TupleQuasiString, new LBucket(TupleQuasiString)); // Create a bucket. Sensitive dataname could also be specified, default is sensitive_class.
            }
            LBucket lb = (LBucket) hashMap.get(TupleQuasiString); // Get the bucket
            lb.add(tuple);

            if (lb.isKAnonymous(k)) { // if bucket satisfies k-anonymity
                if (lb.isLDiverse(l)){
                    AdultData[] tuples = lb.dropBuffer(); // get tuples and drop bucket
                    System.out.println("Releasing bucket! " + tuples[0].QuasiToString(QID));
                    for (AdultData t : tuples) { // output tuples
                        out.collect(t);
                    }
                }
            }
        }
    }


    public static class KAnonymize implements FlatMapFunction<AdultData, AdultData>{
        // K-anonymity
        @Override
        public void flatMap(AdultData tuple, Collector<AdultData> out) throws Exception {
            // get bucket
            String TupleQuasiString = tuple.QuasiToString(QID);
            if (!hashMap.containsKey(TupleQuasiString)) { // If this bucket has never been reached before
                hashMap.put(TupleQuasiString, new Bucket()); // Create a bucket
            }
            Bucket b = hashMap.get(TupleQuasiString); // Get the bucket

            b.add(tuple);
            if (b.isKAnonymous(k)) { // if bucket satisfies k-anonymity
                // b.markAsWorkNode();
                AdultData[] tuples = b.dropBuffer(); // get tuples and drop bucket
                System.out.println("Releasing bucket! " + tuples[0].QuasiToString(QID));
                for (AdultData t : tuples) { // output tuples
                    out.collect(t);
                }
            }
            //}
        }
    }

        /*
    if (b.isWorkNode()) { // If bucket is worknode
        out.collect(tuple); // output tuple

    } else {
    */
}