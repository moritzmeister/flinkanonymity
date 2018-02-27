package org.flinkanonymity.window;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.flink.annotation.PublicEvolving;

/**
 * The default window into which all data is placed (via
 * {@link org.apache.flink.streaming.api.windowing.assigners.GlobalWindows}).
 */
@PublicEvolving
public class UniqueUserWindow extends Window {

    // private static UniqueUserWindow INSTANCE = new UniqueUserWindow();

    private static ConcurrentHashMap<UniqueUserWindow, ConcurrentLinkedQueue<Long>> windows = new ConcurrentHashMap<UniqueUserWindow, ConcurrentLinkedQueue<Long>>();

    private UniqueUserWindow() {}

    public static UniqueUserWindow get(long userId) {
        // for each entry : windows
        UniqueUserWindow window;
        ConcurrentLinkedQueue users;
        for(Map.Entry<UniqueUserWindow, ConcurrentLinkedQueue<Long>> entry : windows.entrySet()) {
            window = entry.getKey();
            users = entry.getValue();

            // if user not in window
            if (!users.contains(userId)) {
                // add userId to window
                users.add(userId);
                //put it back
                windows.put(window,users);
                // return window
                return window;
            }
        }
        // If we've reached this far we could not find a window NOT containing userId
        // Thus, we must create a brand new window.

        // create new window
        window = new UniqueUserWindow();
        users = new ConcurrentLinkedQueue<Long>();
        users.add(userId);

        // add to windows with array[userId]
        windows.put(window, users);
        // return new window
        return window;
    }

    @Override
    public long maxTimestamp() {
        return Long.MAX_VALUE;
    }

    @Override
    public boolean equals(Object o) {
        return this == o || !(o == null || getClass() != o.getClass());
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public String toString() {
        return "UniqueUserWindow";
    }

    /**
     * A {@link TypeSerializer} for {@link UniqueUserWindow}.
     */
    public static class Serializer extends TypeSerializerSingleton<UniqueUserWindow> {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public UniqueUserWindow createInstance() {
            return null;
        }

        @Override
        public UniqueUserWindow copy(UniqueUserWindow from) {
            return from;
        }

        @Override
        public UniqueUserWindow copy(UniqueUserWindow from, UniqueUserWindow reuse) {
            return from;
        }

        @Override
        public int getLength() {
            return 0;
        }

        @Override
        public void serialize(UniqueUserWindow record, DataOutputView target) throws IOException {
            ConcurrentLinkedQueue<Long> users = windows.get(record);
            target.writeLong(users.size());
            for (Long l : users) {
                target.writeLong(l);
            }
        }

        @Override
        public UniqueUserWindow deserialize(DataInputView source) throws IOException {
            Long size = source.readLong();
            ConcurrentLinkedQueue users = new ConcurrentLinkedQueue();
            for (int i = 0; i < size; i++) {
                users.add(source.readLong());
            }
            UniqueUserWindow window = new UniqueUserWindow();
            windows.put(window, users);
            return window;
        }

        @Override
        public UniqueUserWindow deserialize(UniqueUserWindow reuse,
                                            DataInputView source) throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            source.readByte();
            target.writeByte(0);
        }

        @Override
        public boolean canEqual(Object obj) {
            return obj instanceof Serializer;
        }
    }
}
