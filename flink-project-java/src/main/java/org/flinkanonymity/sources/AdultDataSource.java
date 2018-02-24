package org.flinkanonymity.sources;

// Custom classes
import  org.flinkanonymity.datatypes.AdultData;


// Flink
import org.apache.flink.streaming.api.functions.source.SourceFunction;

// Java
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;


public class AdultDataSource implements SourceFunction<AdultData> {

    private final String dataFilePath;
    private transient BufferedReader reader;
    private transient InputStream fileStream;

    public AdultDataSource(String dataFilePath) {
        this.dataFilePath = dataFilePath;
        System.out.println("Construct AdultDataSource");

    }

    @Override
    public void run(SourceContext<AdultData> sourceContext) throws Exception {
        System.out.println("RUN");

        fileStream = new FileInputStream(dataFilePath);
        reader = new BufferedReader(new InputStreamReader(fileStream, "UTF-8"));

        generateStream(sourceContext);

        this.reader.close();
        this.reader = null;
        this.fileStream.close();
        this.fileStream = null;
    }



    private void generateStream(SourceContext<AdultData> sourceContext) throws Exception {
        String line;
        AdultData data;
        System.out.println("Generating stream: ");
        ArrayList<HashMap<String, Integer>> frequencies = new ArrayList<HashMap<String, Integer>>();
        for (int i = 0; i < 11; i ++){
            frequencies.add(new HashMap<String, Integer>());
        }

        while (reader.ready() && (line = reader.readLine()) != null) {
            // read first CensusData
            data = new AdultData(line);
            frequencies = updateFrequencies(frequencies, line);

            // This would also be the place to implement timestamps and such fun..

            // emit data
            //System.out.println(data);
            sourceContext.collect(data);
            // It is also possible to use collectWithTimestamp in order to handle timestamps:
            // https://ci.apache.org/projects/flink/flink-docs-master/api/java/org/apache/flink/streaming/api/functions/source/SourceFunction.SourceContext.html
        }
        while ()

    }

    public String randomAttribute(HashMap<String, Integer> h){
        // Get random number
        int size = 0;
        for(Map.Entry<String, Integer> entry : h.entrySet()) {
            size += entry.getValue();

        }
        int rand = (int)Math.round(Math.random() * size);
        int sum = 0;
        int newsum = 0;
        // for each entry in hashmap
        for(Map.Entry<String, Integer> entry : h.entrySet()) {
            newsum = entry.getValue();
            // if sum + newsum > rand
            if (sum + newsum >= rand){
                // newkey is the random key
                return entry.getKey();
            } else {
                sum += newsum;
            }
        }
        return "Wrong";
    }

    public String createTuple(ArrayList<HashMap<String, Integer>> frequencies){
        StringBuilder sb = new StringBuilder();
        for (HashMap h : frequencies){
            sb.append(randomAttribute(h)).append(";");
        }
        return sb.toString();
    }

    public ArrayList updateFrequencies(ArrayList<HashMap<String, Integer>> frequencies, String line){
        String[] args = line.split(";");
        for (int i = 0; i < args.length; i ++) {
            String arg = args[i];
            frequencies.get(i).putIfAbsent(arg, 0); // Put if absent
            frequencies.get(i).put(arg, frequencies.get(i).get(arg) + 1); // Increment by 1
        }
        return frequencies;
    }

    @Override
    public void cancel() {
        try {
            if (this.reader != null) {
                this.reader.close();
            }
            if (this.fileStream != null) {
                this.fileStream.close();
            }
        } catch(IOException ioe) {
            throw new RuntimeException("Could not cancel SourceFunction", ioe);
        } finally {
            this.reader = null;
            this.fileStream = null;
        }
    }
}