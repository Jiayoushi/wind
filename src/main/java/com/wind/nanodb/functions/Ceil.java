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
 * Computes the smallest whole number larger than the argument.  Returns NULL if 
 * argument is NULL.  The result is always a double-precision number, even
 * though it is a whole number, since this is what {@link Math#ceil} returns.
 */
public class Ceil extends SimpleFunction {
    @Override
    public ColumnType getReturnType(List<Expression> args, Schema schema) {
        // We could try to return an int or long, but Math.ceil() always returns
        // a double, so we'll just return a double too.
        return new ColumnType(SQLDataType.DOUBLE);
    }

    @Override
    public Object evaluate(Environment env, List<Expression> args) {
        if (args.size() != 1) {
            throw new ExpressionException("Cannot call CEIL on " + args.size() 
                    + " arguments");
        }

        Object argVal = args.get(0).evaluate(env);

        if (argVal == null)
            return null;

        return Math.ceil(TypeConverter.getDoubleValue(argVal));
    }
}
