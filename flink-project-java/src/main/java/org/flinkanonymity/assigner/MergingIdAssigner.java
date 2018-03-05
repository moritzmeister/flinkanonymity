package org.flinkanonymity.assigner;

/*
This class is based on the DynamicEventTimeSessionWindows class of Apache Flink, modified to fit the use of IdWindow.
 */


import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.flinkanonymity.datatypes.AdultData;
import org.flinkanonymity.trigger.CustomPurgingTrigger;
import org.flinkanonymity.trigger.lDiversityTrigger;
import org.flinkanonymity.window.IdWindow;

import java.util.Collection;
import java.util.Collections;

/**
 * A {@link WindowAssigner} that windows elements into sessions based on the timestamp of the
 * elements. Windows cannot overlap.
 *
 * <p>For example, in order to window into windows with a dynamic time gap:
 * <pre> {@code
 * DataStream<Tuple2<String, Integer>> in = ...;
 * KeyedStream<String, Tuple2<String, Integer>> keyed = in.keyBy(...);
 * WindowedStream<Tuple2<String, Integer>, String, TimeWindows> windowed =
 *   keyed.window(DynamicEventTimeSessionWindows.withDynamicGap({@link SessionWindowTimeGapExtractor }));
 * } </pre>
 *
 * @param <T> The type of the input elements
 */
@PublicEvolving
public class MergingIdAssigner<T> extends MergingWindowAssigner<Object, IdWindow> {
    private static final long serialVersionUID = 1L;
    private int k;
    private int l;

    public MergingIdAssigner(int k, int l) {
        this.k = k;
        this.l = l;
    }

    @Override
    public Collection<IdWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
        AdultData ad = (AdultData)element;
        return Collections.singletonList(new IdWindow(ad.id));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Trigger<Object, IdWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
//        return CustomPurgingTrigger.of(lDiversityTrigger.of(k, l));
        return lDiversityTrigger.of(k, l);
    }

    @Override
    public String toString() {
        return "DynamicEventTimeSessionWindows()";
    }

    @Override
    public TypeSerializer<IdWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new IdWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return false;
    }

    /**
     * Merge overlapping {@link IdWindow}s.
     */
    public void mergeWindows(Collection<IdWindow> windows, MergeCallback<IdWindow> c) {
        IdWindow.mergeWindows(windows, c);
    }

}