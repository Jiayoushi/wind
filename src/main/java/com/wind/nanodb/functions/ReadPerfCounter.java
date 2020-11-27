package com.wind.nanodb.functions;

import java.util.List;

import com.wind.nanodb.expressions.Environment;
import com.wind.nanodb.expressions.Expression;
import com.wind.nanodb.expressions.ExpressionException;
import com.wind.nanodb.expressions.TypeConverter;

import com.wind.nanodb.relations.ColumnType;
import com.wind.nanodb.relations.SQLDataType;
import com.wind.nanodb.relations.Schema;

import com.wind.nanodb.server.performance.PerformanceCounters;


/** Returns the current value of the specified performance counter. */
public class ReadPerfCounter extends SimpleFunction {
    @Override
    public ColumnType getReturnType(List<Expression> args, Schema schema) {
        return new ColumnType(SQLDataType.INTEGER);
    }


    @Override
    public Object evaluate(Environment env, List<Expression> args) {
        if (args.size() != 1) {
            throw new ExpressionException("Cannot call PERF_COUNTER on " +
                args.size() + " arguments");
        }

        Object argVal = args.get(0).evaluate(env);

        if (argVal == null)
            return null;

        return PerformanceCounters.get(TypeConverter.getStringValue(argVal));
    }
}
