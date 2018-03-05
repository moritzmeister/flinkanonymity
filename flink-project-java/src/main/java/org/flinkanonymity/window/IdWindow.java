package org.flinkanonymity.window;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.ArrayListSerializer;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.Window;
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

    private ArrayList<Long> ids = new ArrayList<Long>();

    /* -- Constructors -- */
    public IdWindow(long id) { this.ids.add(id); }

    public IdWindow(ArrayList<Long> newIds) { ids.addAll(newIds); }

    /* -- Methods -- */

    public boolean contains(Long id) {
        return this.ids.contains(id);
    }

    public int getSize() { return ids.size(); }

    public Long getId(int index){ return ids.get(index); }

    /* -- intersects() and union() are used in the merge process. -- */

    public boolean intersects(IdWindow other) {
        // Method used in the merge process
        for (Long l : other.ids) {
            if(this.ids.contains(l)) {
                // Seems to be working
                System.out.println("INTERSECTING WINDOWS: " + this.ids + " and " + other.ids);
                return true;
            }
        }
        return false;
    }

    public void flush(){
        this.ids = new ArrayList<>();
    }

    public IdWindow merge(IdWindow other) {
        // Returns the union of this and other IdWindow, including duplicates.
        ArrayList<Long> newids = new ArrayList<>();
        Boolean fail = false;

        for (Long id: this.ids){
            if (newids.contains(id)){
                fail = true;
            }
            newids.add(id);
        }
        for (Long id: other.ids){
            if (newids.contains(id)){
                fail = true;
            }
            newids.add(id);
        }
        if(this.intersects(other)){
            fail = true;
        }

        if(fail){
            throw new java.lang.Error("Merging intersectioning windows! ");
        }

        System.out.println("IdWindow.merge() merged: " + this.ids + " and " + other.ids + " into " + newids);
        return new IdWindow(newids);
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
        // return this.ids.hashCode();
        // Function made to be similar to the corresponding function of the TimeWindow
        Long idSum = 0L;
        for (Long l: this.ids){
            idSum += l;
        }
        return MathUtils.longToIntWithBitMixing(idSum);
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
     * The serializer used to write the IdWindow type.
     */
    public static class Serializer extends TypeSerializerSingleton<IdWindow> {
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
            target.writeInt(record.getSize());
            for (int i = 0; i < record.getSize(); i++) {
                target.writeLong(record.getId(i));
            }
        }

        @Override
        public IdWindow deserialize(DataInputView source) throws IOException {
            int size = source.readInt();
            ArrayList<Long> ids = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                ids.add(source.readLong());
            }
            return new IdWindow(ids);
        }

        @Override
        public IdWindow deserialize(IdWindow reuse, DataInputView source) throws IOException { return deserialize(source); }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            int size = source.readInt();
            target.writeInt(size);
            for (int i = 0; i < size; i++) {
                target.writeLong(source.readLong());
            }
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
     * Merge overlapping {@link IdWindow}s. For use by merging
     * {@link org.apache.flink.streaming.api.windowing.assigners.WindowAssigner WindowAssigners}.
     */
    public static void mergeWindows(Collection<IdWindow> windows, MergingWindowAssigner.MergeCallback<IdWindow> c) {

        /*
        This is a reversed version of the TimeWindow implementation of a merge algorithm.
        Instead of merging when two windows intersect, windows are merged when they do not intersect.
        The intersect function has been altered to work with ArrayLists.
        The cover function has been substituted for a merge function.
        */
        List<Tuple2<IdWindow, Set<IdWindow>>> merged = new ArrayList<>();
        Tuple2<IdWindow, Set<IdWindow>> currentMerge = null;

        for (IdWindow candidate: windows) {
            if (currentMerge == null) {
                currentMerge = new Tuple2<>();
                currentMerge.f0 = candidate;
                currentMerge.f1 = new HashSet<>();
                currentMerge.f1.add(candidate);
            } else if (!currentMerge.f0.intersects(candidate)) {
                // If the current merge does NOT intersect with the candidate
                System.out.println("MERGING " + candidate + " with " + currentMerge.f0);
                currentMerge.f0 = currentMerge.f0.merge(candidate);
                currentMerge.f1.add(candidate);
            } else {
                // If the current merge DOES intersect with the candidate
                System.out.println("NOT MERGING " + candidate + " with " + currentMerge.f0);
                //merged.add(currentMerge);
                if (currentMerge.f1.size() > 1) {
                    System.out.println("c.merge(" + currentMerge.f1 + ", " + currentMerge.f0 + " called in else");
                    c.merge(currentMerge.f1, currentMerge.f0);
                }
                currentMerge = new Tuple2<>();
                currentMerge.f0 = candidate;
                currentMerge.f1 = new HashSet<>();
                currentMerge.f1.add(candidate);
            }
        }

        if (currentMerge != null) {
            merged.add(currentMerge);
        }

        for (Tuple2<IdWindow, Set<IdWindow>> m: merged) {
            if (m.f1.size() > 1) {
                System.out.println("c.merge(" + m.f1 + ", " + m.f0);
                c.merge(m.f1, m.f0);
            }
        }
    }
}