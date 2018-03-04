package org.flinkanonymity.process;

import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.shaded.com.google.common.collect.Iterables;

import org.flinkanonymity.datatypes.AdultData;
import org.flinkanonymity.window.IdWindow;
import org.flinkanonymity.window.UniqueUserWindow;

import java.util.ArrayList;

public class Release extends ProcessWindowFunction<AdultData, AdultData, String, IdWindow> {
    @Override
    public void process(String key, Context context, Iterable<AdultData> elements, Collector<AdultData> out) throws Exception {
        System.out.println("Releasing bucket! " + key);
        ArrayList<Long> ids = new ArrayList<>();
        Boolean fail = false;
        Long failId = -1L;
        for (AdultData t: elements) {
            t.setTimestamp("procTimestamp", context.currentProcessingTime());
            if (ids.contains(t.id)){
                fail = true;
                failId = t.id;
            }
            ids.add(t.id);
            out.collect (t);
        }
        System.out.println("Number of records: " + Iterables.size(elements));
        System.out.println("CurrentProcessingTime: " + context.currentProcessingTime());
        System.out.println("CurrentWindow: " + context.window());
        if(fail){
            throw new java.lang.Error("Id " + failId + " occured twice in bucket " + key);
        }
        context.window().flush();
    }
}
