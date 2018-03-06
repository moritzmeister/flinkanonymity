package org.flinkanonymity.jobs;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
//import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction.Context;

import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.flinkanonymity.assigner.MergingIdAssigner;
import org.flinkanonymity.datatypes.*;
import org.flinkanonymity.sources.AdultDataSource;
import org.flinkanonymity.process.Release;
import org.flinkanonymity.process.ProcessTimestamp;


import java.util.HashMap;
import java.util.concurrent.TimeUnit;

public class KeyedJob {
    // Set up QID and Hashmap for global use.
    static QuasiIdentifier QID;
    static HashMap<String, Bucket> hashMap;


    /* Anonymity and Diversity Parameters */
    private static int k = 10;
    private static int l = 4;
    private static int parallelism = 4;

    /* Stream Parameters */
    private static int uniqueAdults = 300;
    private static int streamLength = 20000;


    /* -- Hierarchy Levels -- */
    private static int ageHierarchy = 1;
    private static int sexHierarchy = 0;
    private static int raceHierarchy = 0;
    private static int educHierarchy = 2;
    private static int marstHierarchy = 0;
    private static int countryHierarchy = 1;



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
        Generalization age = new Generalization("age", age_hierarchy, ageHierarchy);
        Generalization sex = new Generalization("sex", sex_hierarchy, sexHierarchy);
        Generalization race = new Generalization("race", race_hierarchy, raceHierarchy);
        Generalization educ = new Generalization("educ", educ_hierarchy,educHierarchy);
        Generalization marst = new Generalization("marst", marst_hierarchy,marstHierarchy);
        Generalization country = new Generalization("country", country_hierarchy,countryHierarchy);
        //Generalization workclass = new Generalization("workclass", workclass_hierarchy,1);

        // Initialize QuasiIdentifier
        QID = new QuasiIdentifier(age, sex, race, educ, marst, country);

        // Setting up Environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        DataStream<AdultData> data = env.addSource(new AdultDataSource(dataFilePath, uniqueAdults, streamLength));

        // Generalize Quasi Identifiers
        DataStream<AdultData> genData = data.map(new Generalize());

        // Write Flink ingestion time into the AdultData objects
        DataStream<AdultData> tsGenData = genData
                .keyBy(new QidKey())
                .process(new ProcessTimestamp());

        DataStream<AdultData> output = tsGenData
                .keyBy(new QidKey())
                .window(new MergingIdAssigner(k, l))
                //.allowedLateness(allowedLateness)
                //.sideOutputLateData(lateOutputTag)
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
}
