package com.wind.nanodb.functions;


import java.util.List;

import com.wind.nanodb.expressions.Environment;
import com.wind.nanodb.expressions.Expression;
import com.wind.nanodb.expressions.ExpressionException;
import com.wind.nanodb.expressions.TypeConverter;

import com.wind.nanodb.relations.ColumnType;
import com.wind.nanodb.relations.Schema;


/**
 * Implements {@code NULLIF(cond, expr)}. Returns {@code NULL} if the first
 * argument is {@code TRUE}, else returns the second argument.
 * 
 * @author emil
 */
public class NullIf extends SimpleFunction {
    @Override
    public ColumnType getReturnType(List<Expression> args, Schema schema) {
        if (args.size() != 2) {
            throw new ExpressionException("Cannot call NULLIF on " +
                args.size() + " arguments");
        }

        // Return the type of the second argument.
        return args.get(1).getColumnInfo(schema).getType();
    }


    @Override
    public Object evaluate(Environment env, List<Expression> args) {
        if (args.size() != 2) {
            throw new ExpressionException("Cannot call NULLIF on " + args.size() 
                    + " arguments");
        }

        Object condVal = args.get(0).evaluate(env);

        if (condVal != null && TypeConverter.getBooleanValue(condVal))
            return null;
        else
            return args.get(1).evaluate(env);
    }

}
