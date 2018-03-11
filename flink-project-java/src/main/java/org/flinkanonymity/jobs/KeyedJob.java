package org.flinkanonymity.jobs;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
//import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction.Context;

import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.flinkanonymity.datatypes.*;
import org.flinkanonymity.keyselector.QidKey;
import org.flinkanonymity.sources.AdultDataSource;
import org.flinkanonymity.process.Release;
import org.flinkanonymity.process.ProcessTimestamp;
import org.flinkanonymity.trigger.lDiversityTrigger;
import org.flinkanonymity.map.Generalize;


import java.util.HashMap;

public class KeyedJob {
    // Set up QID and Hashmap for global use.
    static QuasiIdentifier QID;
    static HashMap<String, Bucket> hashMap;

    /* Anonymity and Diversity Parameters */
    private static int k = 50;
    private static int l = 10;
    private static int parallelism = 10;

    /* Stream Parameters */
    private static int uniqueAdults = 300;
    private static int streamLength = 50000;


    /* -- Hierarchy Levels -- */
    private static int ageHierarchy = 2;
    private static int sexHierarchy = 0;
    private static int raceHierarchy = 0;
    private static int educHierarchy = 2;
    private static int marstHierarchy = 1;
    private static int countryHierarchy = 0;

    private static int[] kvals = {40, 40, 40, 40, 40, 40, 40, 40, 40, 40};
    private static int[] lvals = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    private static int[] pvals = {10, 20, 30, 40};




    public static void main(String[] args) throws Exception {
        for(int i = 0; i < 10; i++){

            k = kvals[i];
            l = lvals[i];
            //parallelism = pvals[i];


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
            QID = new QuasiIdentifier(age, educ, marst);
            //QID = new QuasiIdentifier(age, educ, marst);

            // Setting up Environment
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(parallelism);
            env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
            DataStream<AdultData> data = env.addSource(new AdultDataSource(dataFilePath, uniqueAdults, streamLength, QID));

            // Generalize Quasi Identifiers
            DataStream<AdultData> genData = data.map(new Generalize());

            // Write Flink ingestion time into the AdultData objects
            DataStream<AdultData> tsGenData = genData
                    .keyBy(new QidKey())
                    .process(new ProcessTimestamp());

            DataStream<AdultData> output = tsGenData
                    .keyBy(new QidKey())
                    .window(GlobalWindows.create())
                    .trigger(lDiversityTrigger.of(k, l))
                    .process(new Release());

            output.print();
            String filename = "output-n_" + streamLength + "k_" + k + "l_" + l + "p_" + parallelism + ".csv";
            output.writeAsText("../output/" + filename, org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE).setParallelism(1); // setParalellism(1) yields ONE output file.
            env.execute();
        }
    }
}
