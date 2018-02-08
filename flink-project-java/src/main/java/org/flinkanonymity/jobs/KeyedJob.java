package org.flinkanonymity.jobs;

import org.apache.flink.api.java.operators.translation.PlanFilterOperator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.OutputTag;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
//import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction.Context;
import org.apache.flink.api.java.tuple.Tuple2;

import org.flinkanonymity.datatypes.*;
import org.flinkanonymity.sources.AdultDataSource;

import java.util.HashMap;

public class KeyedJob {
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
        //env.setParallelism(2);
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        DataStream<AdultData> data = env.addSource(new AdultDataSource(dataFilePath));

        // DataStreamSink<AdultData> output = new DataStreamSink<AdultData>();
        // output.setParallelism(1);

        // Generalize Quasi Identifiers
        DataStream<AdultData> genData = data.map(new Generalize());

/*
        KeyedStream<AdultData, String> keyedGenData = genData.keyBy(new KeySelector<AdultData, String>() {
            public String getKey(AdultData tuple) {
                String TupleQuasiString = tuple.QuasiToString(QID);
                return TupleQuasiString;
            }
        });
*/
/*
        DataStream<Integer> output = genData
                .keyBy(new KeySelector<AdultData, String>() {
                    public String getKey(AdultData tuple) {
                        String TupleQuasiString = tuple.QuasiToString(QID);
                        return TupleQuasiString;
                    }
                })
                .countWindow(k)
                .apply(new WindowFunction<AdultData, Integer, String, GlobalWindow>() {
                    public void apply (String key, GlobalWindow window, Iterable<AdultData> values, Collector<Integer> out) throws Exception {
                        int count = 0;
                        for (AdultData t: values) {
                            count += 1;
                        }
                        out.collect (new Integer(count));
                    }
                });
*/
        DataStream<Tuple2<Long, AdultData>> output = genData
                .keyBy(new QidKey())
                .countWindow(k)
                .process(new KAnonymize2());
                //.apply(new KAnonymize());

        output.print();

        //genData.print();
        env.execute();
    }


    public static class KAnonymize2 extends ProcessWindowFunction<AdultData, Tuple2<Long, AdultData>, String, GlobalWindow> {
        @Override
        public void process(String key, Context context, Iterable<AdultData> elements, Collector<Tuple2<Long, AdultData>> out) throws Exception {
            int count = 0;
            System.out.println("Releasing bucket! " + key);
            for (AdultData t: elements) {
                count += 1;
                out.collect (new Tuple2<Long, AdultData>(t.getCreationTime(), t));
            }
            //tag += 1;
            System.out.println("Number of records: " + count);
            System.out.println("CurrentProcessingTime: " + context.currentProcessingTime());
            System.out.println("CurrentWindow: " + context.window());
        }
    }

    public static class KAnonymize implements WindowFunction<AdultData, AdultData, String, GlobalWindow> {
        @Override
        public void apply (String key, GlobalWindow window, Iterable<AdultData> values, Collector<AdultData> out) throws Exception {
            int count = 0;
            System.out.println("Releasing bucket! " + key);
            for (AdultData t: values) {
                count += 1;
                out.collect (t);
            }
            //tag += 1;
            System.out.println("Number of records: " + count);
        }
    }

    public static class QidKey implements KeySelector<AdultData, String> {
        @Override
        public String getKey(AdultData tuple) {
            String TupleQuasiString = tuple.QuasiToString(QID);
            return TupleQuasiString;
        }
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
}