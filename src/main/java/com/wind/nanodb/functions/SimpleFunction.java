package com.wind.nanodb.functions;


import java.util.List;

import com.wind.nanodb.expressions.Environment;
import com.wind.nanodb.expressions.Expression;
import com.wind.nanodb.expressions.ExpressionException;


/**
 * Created by donnie on 12/5/13.
 */
public abstract class SimpleFunction extends ScalarFunction {
    /**
     * Evaluates the function.
     *
     * Should be called at runtime for every function call.
     *
     * @param env Environment, in which the arguments are evaluated
     * @param args Arguments for this function
     *
     * @return The value of the function
     *
     * @throws ExpressionException when there is a problem with the evaluation
     *                             like wrong number of arguments.
     */
    public abstract Object evaluate(Environment env, List<Expression> args);
}
