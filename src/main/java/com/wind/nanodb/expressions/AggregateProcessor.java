package com.wind.nanodb.expressions;

import com.wind.nanodb.functions.AggregateFunction;
import com.wind.nanodb.functions.Function;

import java.util.HashMap;
import java.util.Map;

public class AggregateProcessor implements ExpressionProcessor {

    private static final String mark = "#";

    private Map<String, FunctionCall> columnToFunctionCall;

    public AggregateProcessor() {
        columnToFunctionCall = new HashMap<>();
    }

    @Override
    public void enter(Expression e) {
        System.out.printf("Enter %s\n", e.toString());
        if (e instanceof FunctionCall) {
            FunctionCall call = (FunctionCall) e;
            Function f = call.getFunction();
            if (f instanceof AggregateFunction) {
                System.out.printf("  FunctionCall: args(%s) f(%s)\n", call.getArguments().get(0), call.toString());
                String columnName = mark + call.toString();
                columnToFunctionCall.putIfAbsent(columnName, call);
            }
        }
    }

    @Override
    public Expression leave(Expression e) {
        System.out.printf("Leave %s\n", e.toString());
        if (e instanceof FunctionCall) {
            FunctionCall call = (FunctionCall) e;
            Function f = call.getFunction();
            if (f instanceof AggregateFunction) {
                String columnName = mark + call.toString();
                if (columnToFunctionCall.containsKey(columnName)) {
                    e = new ColumnValue(new ColumnName(columnName));
                }
            }
        }
        return e;
    }

    public Map<String, FunctionCall> getAggregates() {
        return columnToFunctionCall;
    }

    public boolean hasAggregates() {
        return columnToFunctionCall.size() > 0;
    }
}
