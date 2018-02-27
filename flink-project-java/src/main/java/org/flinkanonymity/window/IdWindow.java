package org.flinkanonymity.window;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.util.MathUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@PublicEvolving
public class IdWindow extends Window {

    private final ArrayList<Long> ids = new ArrayList<Long>();

    public IdWindow(long id) {
        this.ids.add(id);
    }

    public boolean doesContain(Long id) {
        return this.ids.contains(id);
    }

    @Override
    public long maxTimestamp() {
        return Long.MAX_VALUE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IdWindow window = (IdWindow) o;

        Collections.sort(ids);
        Collections.sort(window.ids);

        return ids.equals(window.ids);
    }

    @Override
    public int hashCode() {
        return this.ids.hashCode();
    }

    @Override
    public String toString() {
        return "IdWindow{" +
                this.ids.toString() +
                '}';
    }

    // ------------------------------------------------------------------------
    // Serializer
    // ------------------------------------------------------------------------

    /**
     * The serializer used to write the TimeWindow type.
     */
    public static class Serializer extends ArrayListSerializer<IdWindow> {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public IdWindow createInstance() {
            return null;
        }

        @Override
        public IdWindow copy(IdWindow from) {
            return from;
        }

        @Override
        public IdWindow copy(IdWindow from, IdWindow reuse) {
            return from;
        }

        @Override
        public int getLength() {
            return 0;
        }

        @Override
        public void serialize(IdWindow record, DataOutputView target) throws IOException {
            target.writeByte(0);
        }

        @Override
        public TimeWindow deserialize(DataInputView source) throws IOException {
            long start = source.readLong();
            long end = source.readLong();
            return new TimeWindow(start, end);
        }

        @Override
        public TimeWindow deserialize(TimeWindow reuse, DataInputView source) throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            target.writeLong(source.readLong());
            target.writeLong(source.readLong());
        }

        @Override
        public boolean canEqual(Object obj) {
            return obj instanceof Serializer;
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    /**
     * Merge overlapping {@link TimeWindow}s. For use by merging
     * {@link org.apache.flink.streaming.api.windowing.assigners.WindowAssigner WindowAssigners}.
     */
    public static void mergeWindows(Collection<TimeWindow> windows, MergingWindowAssigner.MergeCallback<TimeWindow> c) {

        // sort the windows by the start time and then merge overlapping windows

        List<TimeWindow> sortedWindows = new ArrayList<>(windows);

        Collections.sort(sortedWindows, new Comparator<TimeWindow>() {
            @Override
            public int compare(TimeWindow o1, TimeWindow o2) {
                return Long.compare(o1.getStart(), o2.getStart());
            }
        });

        List<Tuple2<TimeWindow, Set<TimeWindow>>> merged = new ArrayList<>();
        Tuple2<TimeWindow, Set<TimeWindow>> currentMerge = null;

        for (TimeWindow candidate: sortedWindows) {
            if (currentMerge == null) {
                currentMerge = new Tuple2<>();
                currentMerge.f0 = candidate;
                currentMerge.f1 = new HashSet<>();
                currentMerge.f1.add(candidate);
            } else if (currentMerge.f0.intersects(candidate)) {
                currentMerge.f0 = currentMerge.f0.cover(candidate);
                currentMerge.f1.add(candidate);
            } else {
                merged.add(currentMerge);
                currentMerge = new Tuple2<>();
                currentMerge.f0 = candidate;
                currentMerge.f1 = new HashSet<>();
                currentMerge.f1.add(candidate);
            }
        }

        if (currentMerge != null) {
            merged.add(currentMerge);
        }

        for (Tuple2<TimeWindow, Set<TimeWindow>> m: merged) {
            if (m.f1.size() > 1) {
                c.merge(m.f1, m.f0);
            }
        }
    }

    /**
     * Method to get the window start for a timestamp.
     *
     * @param timestamp epoch millisecond to get the window start.
     * @param offset The offset which window start would be shifted by.
     * @param windowSize The size of the generated windows.
     * @return window start
     */
    public static long getWindowStartWithOffset(long timestamp, long offset, long windowSize) {
        return timestamp - (timestamp - offset + windowSize) % windowSize;
    }
}}