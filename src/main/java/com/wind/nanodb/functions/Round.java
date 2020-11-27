package com.wind.nanodb.functions;


import java.util.List;

import com.wind.nanodb.relations.ColumnInfo;
import com.wind.nanodb.relations.ColumnType;
import com.wind.nanodb.relations.SQLDataType;
import com.wind.nanodb.relations.Schema;

import com.wind.nanodb.expressions.Environment;
import com.wind.nanodb.expressions.Expression;
import com.wind.nanodb.expressions.ExpressionException;
import com.wind.nanodb.expressions.TypeConverter;


/**
 * Computes the whole number that is closest to the argument.  Returns NULL if
 * argument is NULL.
 * 
 * @author emil
 */
public class Round extends SimpleFunction {
    @Override
    public ColumnType getReturnType(List<Expression> args, Schema schema) {
        if (args.size() < 1) {
            throw new ExpressionException("Cannot call ROUND on " +
                args.size() + " arguments");
        }

        // Return the type of the first argument.
        ColumnInfo argInfo = args.get(0).getColumnInfo(schema);
        SQLDataType argBaseType = argInfo.getType().getBaseType();

        SQLDataType retBaseType = SQLDataType.BIGINT;
        if (argBaseType == SQLDataType.FLOAT)
            retBaseType = SQLDataType.INTEGER;

        return new ColumnType(retBaseType);
    }


    @Override
    public Object evaluate(Environment env, List<Expression> args) {
        if (args.size() != 1) {
            throw new ExpressionException("Cannot call ROUND on " + args.size() 
                    + " arguments");
        }

        Object argVal = args.get(0).evaluate(env);
        
        if (argVal == null)
            return null;

        // If the argument is a float, return an int.
        if (argVal instanceof Float)
            return Math.round((Float) argVal);

        // Otherwise, return a long.
        return Math.round(TypeConverter.getDoubleValue(argVal));
    }
}
