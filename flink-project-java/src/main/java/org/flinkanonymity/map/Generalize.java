package org.flinkanonymity.map;

import org.apache.flink.api.common.functions.MapFunction;

import org.flinkanonymity.datatypes.AdultData;

public class Generalize implements MapFunction<AdultData, AdultData>{
    @Override
    public AdultData map(AdultData adult) throws Exception{
        return adult.QID.generalize(adult);
    }
}