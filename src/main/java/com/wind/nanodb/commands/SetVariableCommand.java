package com.wind.nanodb.commands;


import com.wind.nanodb.server.NanoDBServer;
import com.wind.nanodb.expressions.Expression;
import com.wind.nanodb.expressions.ExpressionException;
import com.wind.nanodb.server.properties.PropertyRegistry;
import com.wind.nanodb.server.properties.ReadOnlyPropertyException;
import com.wind.nanodb.server.properties.UnrecognizedPropertyException;


/**
 * Implements the "SET VARIABLE ..." command.
 */
public class SetVariableCommand extends Command {


    private String propertyName;


    private Expression valueExpr;


    public SetVariableCommand(String propertyName, Expression valueExpr) {
        super(Command.Type.UTILITY);

        this.propertyName = propertyName;
        this.valueExpr = valueExpr;
    }


    @Override
    public void execute(NanoDBServer server) throws ExecutionException {

        try {
            PropertyRegistry propReg = server.getPropertyRegistry();
            Object value = valueExpr.evaluate();
            propReg.setPropertyValue(propertyName, value);
            out.printf("Set property \"%s\" to value %s%n", propertyName, value);
        }
        catch (UnrecognizedPropertyException e) {
            throw new ExecutionException(e);
        }
        catch (ReadOnlyPropertyException e) {
            throw new ExecutionException(e);
        }
        catch (ExpressionException e) {
            throw new ExecutionException(e);
        }
    }
}
