package org.flinkanonymity.assigner;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.flinkanonymity.datatypes.AdultData;
import org.flinkanonymity.trigger.CustomPurgingTrigger;
import org.flinkanonymity.window.IdWindow;
import org.flinkanonymity.window.UniqueUserWindow;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.flinkanonymity.trigger.lDiversityTrigger;

import java.util.Collection;
import java.util.Collections;

/**
 * A {@link WindowAssigner} that assigns all elements to the same {@link GlobalWindow}.
 *
 * <p>Use this if you want to use a {@link Trigger} and
 * {@link org.apache.flink.streaming.api.windowing.evictors.Evictor} to do flexible, policy based
 * windows.
 */
@PublicEvolving
public class IdAssigner extends WindowAssigner<Object, IdWindow> {
    private static final long serialVersionUID = 1L;
    private int k;
    private int l;

    private IdAssigner(int k, int l) {
        this.k = k;
        this.l = l;
    }

    @Override
    public Collection<IdWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
        AdultData tuple = (AdultData)element;
        return Collections.singletonList(new IdWindow(tuple.id));
    }

    @Override
    public Trigger<Object, IdWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
        //return CustomPurgingTrigger.of(lDiversityTrigger.of(k, l));
        return CustomPurgingTrigger.of(lDiversityTrigger.of(k, l));

    }

    @Override
    public String toString() {
        return "GlobalWindows()";
    }

    /**
     * Creates a new {@code GlobalWindows} {@link WindowAssigner} that assigns
     * all elements to the same {@link GlobalWindow}.
     *
     * @return The global window policy.
     */
    public static IdAssigner create(int k, int l) {
        return new IdAssigner(k, l);
    }


    @Override
    public TypeSerializer<IdWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new IdWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return false;
    }
}