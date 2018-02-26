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
    private int streamLength;
    private int uniqueAdults;


    public AdultDataSource(String dataFilePath, int uniqueAdults, int streamLength) {
        this.dataFilePath = dataFilePath;
        this.uniqueAdults = uniqueAdults;
        this.streamLength = streamLength;
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
        long idCount = 0L;
        System.out.println("Generating stream: ");
        ArrayList<HashMap<String, Integer>> frequencies = new ArrayList<HashMap<String, Integer>>();
        for (int i = 0; i < 12; i ++){
            frequencies.add(new HashMap<String, Integer>());
        }

        while (reader.ready() && (line = reader.readLine()) != null) {
            // read first CensusData
            //data = new AdultData(line);
            // idCount = data.id;
            frequencies = updateFrequencies(frequencies, line);

            // This would also be the place to implement timestamps and such fun..

            // emit data
            //System.out.println(data);
            // sourceContext.collect(data);
            // It is also possible to use collectWithTimestamp in order to handle timestamps:
            // https://ci.apache.org/projects/flink/flink-docs-master/api/java/org/apache/flink/streaming/api/functions/source/SourceFunction.SourceContext.html
        }

        // Generate sample adultdata objects with same distribution as source data.
        for (int i = 0; i < streamLength; i++){
            //idCount += 1L;
            long randId = (long)(Math.random()*uniqueAdults);
            line = createTuple(frequencies, (randId));
            data = new AdultData(line);
            sourceContext.collect(data);
        }
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

    public String createTuple(ArrayList<HashMap<String, Integer>> frequencies, long id){
        StringBuilder sb = new StringBuilder();
        sb.append(id);
        int count = 0;
        for (HashMap h : frequencies){
            if (count > 0){
                sb.append(";");
                sb.append(randomAttribute(h));
            }
            count++;
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