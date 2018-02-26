package org.flinkanonymity.trigger;

import org.apache.flink.shaded.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.functions.ReduceFunction;

import org.flinkanonymity.datatypes.AdultData;

public class lDiversityTrigger<W extends Window> extends Trigger<Object, W> {

    private final int l, k;
    private String sensitive = "sensitive_class";


    private final MapStateDescriptor<String, Integer> stateMap =
            new MapStateDescriptor<>("map", StringSerializer.INSTANCE, IntSerializer.INSTANCE);

    private final ReducingStateDescriptor<Long> stateDesc =
            new ReducingStateDescriptor<>("count", new Sum(), LongSerializer.INSTANCE);


    private lDiversityTrigger(int k, int l) {
        // Constructor
        this.k = k;
        this.l = l;
    }

    public static <W extends Window> lDiversityTrigger<W> of(int k, int l) {
        return new lDiversityTrigger<>(k, l);
    }

    @Override
    public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) throws Exception {
        // Object element is the tuple
        AdultData ad = (AdultData)element;
        // Get sensitive data which will serve as key
        String sensitiveData = ad.getAttribute(this.sensitive);
        // Get the MapState, which is a map with keys and counters.
        MapState<String, Integer> diversityMap = ctx.getPartitionedState(stateMap);
        ReducingState<Long> count = ctx.getPartitionedState(stateDesc);

        // Add 1 to the counter to keep track of number of elements in window.
        count.add(1L);

        // Add a tuple with the sensitiveData as key, in order to keep track of diversity.
        diversityMap.put(sensitiveData, 1);

        if (count.get() >= this.k){
            // If number of tuples > k (If k-anonymous)
            if(Iterables.size(diversityMap.keys()) >= this.l){
                // If number of keys > l (If l-diverse)
                diversityMap.clear();
                count.clear();
                return TriggerResult.FIRE;
            }
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, W window, TriggerContext ctx) {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(W window, TriggerContext ctx) throws Exception {
        ctx.getPartitionedState(stateMap).clear();
    }
    private static class Sum implements ReduceFunction<Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public Long reduce(Long value1, Long value2) throws Exception {
            return value1 + value2;
        }

    }

}