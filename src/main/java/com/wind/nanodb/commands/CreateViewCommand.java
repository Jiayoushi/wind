package com.wind.nanodb.commands;


import com.wind.nanodb.server.NanoDBServer;
import com.wind.nanodb.queryast.SelectClause;


/**
 * This command-class represents the <tt>CREATE VIEW</tt> DDL command.
 */
public class CreateViewCommand extends Command {

    private String viewName;

    private SelectClause selectClause;


    public CreateViewCommand(String viewName, SelectClause selectClause) {
        super(Type.DDL);

        if (viewName == null)
            throw new IllegalArgumentException("viewName cannot be null");

        if (selectClause == null)
            throw new IllegalArgumentException("selectClause cannot be null");

        this.viewName = viewName;
        this.selectClause = selectClause;
    }


    @Override
    public void execute(NanoDBServer server)
        throws ExecutionException {
        throw new ExecutionException("Not yet implemented!");
    }
}
