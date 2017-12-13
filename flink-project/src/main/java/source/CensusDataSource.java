package source;


// Custom classes
import  datatypes.CensusData;


// Flink
import org.
import org.apache.flink.streaming.api.functions.source.*;



// Java
import java.io.BufferedReader;


public class CensusDataSource implements SourceFunction<CensusData> {

    private final String dataFilePath;

    private transient BufferedReader reader;

    public CensusDataSource(String dataFilePath) {
        this.dataFilePath = dataFilePath;
    }

    public void run(SourceContext<CensusData> sourceContext) throws Exception {

    }

}
