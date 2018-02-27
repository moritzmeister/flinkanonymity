package org.flinkanonymity.jobs;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.CustomAssigner;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.OutputTag;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
//import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction.Context;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;

import org.flinkanonymity.datatypes.*;
import org.flinkanonymity.sources.AdultDataSource;
import org.flinkanonymity.process.Release;
import org.flinkanonymity.trigger.lDiversityTrigger;
import org.flinkanonymity.process.ProcessTimestamp;


import java.util.HashMap;

public class KeyedJob {
    // Set up QID and Hashmap for global use.
    static QuasiIdentifier QID;
    static HashMap<String, Bucket> hashMap;
    static int k = 10;
    static int l = 5;
    static int p = 5;
    static int uniqueAdults = 5000;
    static int streamLength = 20000;


    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        // final String filePath = params.getRequired("input");

        // Set data file path
        String dataFilePath = "../sample-data/arx_adult/adult_sensitive.csv";

        // Set Hierarchy files paths.
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

        // Initialize QuasiIdentifier
        QID = new QuasiIdentifier(age, sex, race, educ, marst, country);

        // Setting up Environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(p);
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        DataStream<AdultData> data = env.addSource(new AdultDataSource(dataFilePath, streamLength, uniqueAdults));

/* - Some manual calculations of timestamps
        DataStream<Tuple2<AdultData, Long>> tsGenData = genData
                .keyBy(new QidKey())
                .process(new ProcessTimestamp());

        tsGenData.print();
*/
        // DataStream<AdultData> output = genData;
        // DataStreamSink<AdultData> output = new DataStreamSink<AdultData>();

        // Generalize Quasi Identifiers
        DataStream<AdultData> genData = data.map(new Generalize());

        // Write Flink ingestion time into the AdultData objects
        DataStream<AdultData> tsGenData = genData
                .keyBy(new QidKey())
                .process(new ProcessTimestamp());

        DataStream<AdultData> output = tsGenData
                .keyBy(new QidKey())
                .window(CustomAssigner.create(k, l))
                //.trigger(PurgingTrigger.of(lDiversityTrigger.of(k, l)))
                .process(new Release());

        output.print();
        output.writeAsText("../output/test.csv", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        env.execute();
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
            // Use arx to generalize
            return QID.generalize(adult);
        }
    }
/*
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
*/
}
