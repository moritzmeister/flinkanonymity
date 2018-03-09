package org.flinkanonymity.process;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import org.flinkanonymity.datatypes.AdultData;

public class ProcessTimestamp extends ProcessFunction<AdultData, AdultData> {
    // Transfrom a DataStream of AdultData elements into a stream of AdultData elements with ingestion timestamp
    @Override
    public void processElement(AdultData value, Context ctx, Collector<AdultData> out) throws Exception {
        value.setTimestamp("ingTimestamp", ctx.timestamp());
        out.collect(value);
    }
}