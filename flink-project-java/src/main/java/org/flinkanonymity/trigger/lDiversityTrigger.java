package org.flinkanonymity.trigger;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.functions.ReduceFunction;

import org.flinkanonymity.datatypes.AdultData;
import org.flinkanonymity.window.IdWindow;

import java.util.ArrayList;

public class lDiversityTrigger<W extends IdWindow> extends Trigger<Object, W> {

    private final int l, k;
    private String sensitive = "sensitive_class";
    private final ReducingStateDescriptor<String> stateString =
            new ReducingStateDescriptor<>("string", new Sum(), StringSerializer.INSTANCE);


    private lDiversityTrigger(int k, int l) {
        this.k = k;
        this.l = l;
    }

    public static <W extends IdWindow> lDiversityTrigger<W> of(int k, int l) {
        return new lDiversityTrigger<>(k, l);
    }

    @Override
    public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) throws Exception {
        // Object element is the tuple
        AdultData ad = (AdultData)element;
        // Get sensitive data which will serve as key

        ReducingState<String> sensValues = ctx.getPartitionedState(stateString);
        sensValues.add(ad.getAttribute(this.sensitive));

        if (this.getSize(sensValues.get()) >= this.k){
            // If number of tuples > k (If k-anonymous)
            if(this.getDiversity(sensValues.get()) >= this.l){
                // If number of keys > l (If l-diverse)
                sensValues.clear();
                return TriggerResult.FIRE_AND_PURGE;
            }
        }
        return TriggerResult.CONTINUE;
    }

    private int getDiversity(String sens){
        String[] sensitiveAttributes = sens.split(",");
        ArrayList<String> diverseList = new ArrayList<>();
        for (String s: sensitiveAttributes){
            if (!diverseList.contains(s)){
                diverseList.add(s);
            }
        }
        return diverseList.size();
    }

    private int getSize(String sens){ return sens.split(",").length;}

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
        ctx.getPartitionedState(stateString).clear();
    }

    private static class Sum implements ReduceFunction<String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String reduce(String value1, String value2) throws Exception {
            return value1 + "," + value2;
        }

    }
    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(W window, OnMergeContext ctx) throws Exception {
        ctx.mergePartitionedState(stateString);
    }
}
