package com.wind.nanodb.functions;


/**
 * Created by donnie on 12/7/13.
 */
public class Min extends MinMaxAggregate {
    public Min() {
        super(/* minimum */ true);
    }
}
