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
 * Concatenates arguments as strings.  If any of the arguments is NULL, returns
 * NULL.
 *
 * @author emil
 */
public class Concat extends SimpleFunction {
    @Override
    public ColumnType getReturnType(List<Expression> args, Schema schema) {
        return new ColumnType(SQLDataType.VARCHAR);
    }

    @Override
    public Object evaluate(Environment env, List<Expression> args) {
        if (args.size() < 2) {
            throw new ExpressionException("CONCAT requires at least 2 " +
                "arguments; got " + args.size());
        }

        StringBuilder buf = new StringBuilder();

        for (Expression arg : args) {
            Object val = arg.evaluate(env);

            if (val == null)
                return null;

            buf.append(TypeConverter.getStringValue(val));
        }

        return buf.toString();
    }
}
