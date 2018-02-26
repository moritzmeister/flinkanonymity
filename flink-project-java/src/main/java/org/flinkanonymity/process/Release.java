package org.flinkanonymity.process;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.shaded.com.google.common.collect.Iterables;

import org.flinkanonymity.datatypes.AdultData;

public class Release extends ProcessWindowFunction<AdultData, AdultData, String, GlobalWindow> {
    @Override
    public void process(String key, Context context, Iterable<AdultData> elements, Collector<AdultData> out) throws Exception {
        System.out.println("Releasing bucket! " + key);
        for (AdultData t: elements) {
            t.setTimestamp("procTimestamp", context.currentProcessingTime());
            out.collect (t);
        }
        System.out.println("Number of records: " + Iterables.size(elements));
        System.out.println("CurrentProcessingTime: " + context.currentProcessingTime());
        System.out.println("CurrentWindow: " + context.window());
    }
}
