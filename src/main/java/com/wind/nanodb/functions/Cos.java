package com.wind.nanodb.functions;


import java.util.List;

import com.wind.nanodb.expressions.Environment;
import com.wind.nanodb.expressions.Expression;
import com.wind.nanodb.expressions.ExpressionException;
import com.wind.nanodb.expressions.TypeConverter;

import com.wind.nanodb.relations.ColumnType;
import com.wind.nanodb.relations.SQLDataType;
import com.wind.nanodb.relations.Schema;


/**
 * Computes the cosine of a single argument.  Returns NULL if argument
 * is NULL.
 * 
 * @author emil
 */
public class Cos extends SimpleFunction {
    @Override
    public ColumnType getReturnType(List<Expression> args, Schema schema) {
        return new ColumnType(SQLDataType.DOUBLE);
    }

    @Override
    public Object evaluate(Environment env, List<Expression> args) {
        if (args.size() != 1) {
            throw new ExpressionException("Cannot call COS on " + args.size() +
                " arguments");
        }

        Object argVal = args.get(0).evaluate(env);
        
        if (argVal == null)
            return null;

        return Math.cos(TypeConverter.getDoubleValue(argVal));
    }

}
