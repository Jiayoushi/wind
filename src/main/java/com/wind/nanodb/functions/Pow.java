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
 * Returns the first argument, raised to the second argument power. If any
 * of the arguments is {@code NULL}, returns {@code NULL}.
 * 
 * @author emil
 */
public class Pow extends SimpleFunction {
    @Override
    public ColumnType getReturnType(List<Expression> args, Schema schema) {
        return new ColumnType(SQLDataType.DOUBLE);
    }


    @Override
    public Object evaluate(Environment env, List<Expression> args) {
        if (args.size() != 2) {
            throw new ExpressionException("Cannot call POW on " + args.size() +
                " arguments");
        }

        Object argVal1 = args.get(0).evaluate(env);
        Object argVal2 = args.get(1).evaluate(env);
   
        if (argVal1 == null || argVal2 == null)
            return null;

        return Math.pow(TypeConverter.getDoubleValue(argVal1),
                        TypeConverter.getDoubleValue(argVal2));
    }
}
