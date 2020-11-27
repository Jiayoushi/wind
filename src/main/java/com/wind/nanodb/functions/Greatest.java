package com.wind.nanodb.functions;


import java.util.List;

import com.wind.nanodb.expressions.Environment;
import com.wind.nanodb.expressions.Expression;
import com.wind.nanodb.expressions.ExpressionException;

import com.wind.nanodb.relations.ColumnType;
import com.wind.nanodb.relations.Schema;


/**
 * Returns the greatest value of all arguments.  <ttNULL</tt> arguments are
 * ignored; the function only produces <tt>NULL</tt> if all inputs are
 * <tt>NULL</tt>.  The type of the result is the type of the first argument.
 */
public class Greatest extends SimpleFunction {
    @Override
    public ColumnType getReturnType(List<Expression> args, Schema schema) {
        if (args.size() < 2) {
            throw new ExpressionException("Cannot call GREATEST on " +
                args.size() + " arguments");
        }

        // Return the type of the first argument.
        return args.get(0).getColumnInfo(schema).getType();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object evaluate(Environment env, List<Expression> args) {
        if (args.size() < 2) {
            throw new ExpressionException("Cannot call GREATEST on " +
                args.size() + " arguments");
        }
        
        Comparable greatest = null;
        for (Expression arg : args) {
            Comparable val = (Comparable) arg.evaluate(env);
            if (val == null)
                continue;

            if (greatest == null || val.compareTo(greatest) > 0)
                greatest = val;
        }

        return greatest;
    }
}
