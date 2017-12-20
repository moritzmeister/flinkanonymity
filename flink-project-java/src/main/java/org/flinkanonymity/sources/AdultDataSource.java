package org.flinkanonymity.sources;

// Custom classes
import  org.flinkanonymity.datatypes.AdultData;


// Flink
import org.apache.flink.streaming.api.functions.source.SourceFunction;

// Java
import java.io.*;
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


        while (reader.ready() && (line = reader.readLine()) != null) {
            // read first CensusData
            data = new AdultData(line);

            // This would also be the place to implement timestamps and such fun..

            // emit data
            //System.out.println(data);
            sourceContext.collect(data);
            // It is also possible to use collectWithTimestamp in order to handle timestamps:
            // https://ci.apache.org/projects/flink/flink-docs-master/api/java/org/apache/flink/streaming/api/functions/source/SourceFunction.SourceContext.html

        }

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