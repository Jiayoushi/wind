package com.wind.nanodb.functions;


import java.util.List;

import com.wind.nanodb.expressions.Environment;
import com.wind.nanodb.expressions.Expression;
import com.wind.nanodb.expressions.ExpressionException;

import com.wind.nanodb.relations.ColumnType;
import com.wind.nanodb.relations.Schema;


/**
 * Returns the first non-<tt>NULL</tt> argument.  The function's return-type is
 * reported to be whatever the first argument's type is.
 * 
 * @author emil
 */
public class Coalesce extends SimpleFunction {
    @Override
    public ColumnType getReturnType(List<Expression> args, Schema schema) {
        if (args.size() < 1) {
            throw new ExpressionException("Cannot call COALESCE on " +
                args.size() + " arguments");
        }

        // Return the type of the first argument.
        return args.get(0).getColumnInfo(schema).getType();
    }

    @Override
    public Object evaluate(Environment env, List<Expression> args) {
        if (args.size() < 1) {
            throw new ExpressionException("Cannot call COALESCE on " +
                args.size() + " arguments");
        }
        
        for (Expression arg : args) {
            Object val = arg.evaluate(env);
            
            if (val != null)
                return val;
        }
        
        return null;
    }
}
