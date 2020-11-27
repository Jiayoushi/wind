package com.wind.nanodb.functions;


import java.util.List;

import com.wind.nanodb.expressions.Expression;
import com.wind.nanodb.relations.ColumnType;
import com.wind.nanodb.relations.Schema;


/** This class represents functions that return a scalar value. */
public abstract class ScalarFunction extends Function {
    /**
     * Returns the column type of the resulting column after applying the
     * function. This generally depends on the column type of the inputs.
     *
     * @param args the arguments to the function call
     * @param schema the schema of the table
     * @return the column type of the resulting column
     */
    public abstract ColumnType getReturnType(List<Expression> args, Schema schema);
}
