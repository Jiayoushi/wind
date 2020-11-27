package com.wind.nanodb.functions;


/**
 * Created by donnie on 12/7/13.
 */
public class Avg extends SumAvgAggregate {
    public Avg() {
        super(/* computeAverage */ true, /* distinct */ false);
    }
}
