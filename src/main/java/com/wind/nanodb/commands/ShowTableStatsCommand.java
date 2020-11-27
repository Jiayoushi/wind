package com.wind.nanodb.commands;


import java.io.IOException;

import com.wind.nanodb.server.NanoDBServer;
import com.wind.nanodb.storage.StorageManager;
import com.wind.nanodb.storage.TableManager;
import com.wind.nanodb.expressions.ColumnName;
import com.wind.nanodb.queryeval.ColumnStats;
import com.wind.nanodb.queryeval.TableStats;
import com.wind.nanodb.relations.TableInfo;
import com.wind.nanodb.relations.TableSchema;


/**
 * Implements the "SHOW TABLE t STATS" command.
 */
public class ShowTableStatsCommand extends Command {

    /** The name of the table whose statistics to print out. */
    private String tableName;


    public ShowTableStatsCommand(String tableName) {
        super(Command.Type.UTILITY);
        this.tableName = tableName;
    }


    @Override
    public void execute(NanoDBServer server) throws ExecutionException {
        try {
            StorageManager storageManager = server.getStorageManager();
            TableManager tableManager = storageManager.getTableManager();
            TableInfo tableInfo = tableManager.openTable(tableName);

            TableSchema schema = tableInfo.getSchema();
            TableStats stats = tableInfo.getStats();
            out.printf("Statistics for table %s:%n", tableName);
            out.printf("\t%d tuples, %d data pages, avg tuple size is %.1f bytes%n",
                stats.numTuples, stats.numDataPages, stats.avgTupleSize);

            int numCols = schema.numColumns();
            for (int i = 0; i < numCols; i++) {
                ColumnName colName = schema.getColumnInfo(i).getColumnName();
                ColumnStats colStat = stats.getColumnStats(i);

                out.printf("\tColumn %s:  %d unique values, %d null " +
                    "values, min = %s, max = %s%n", colName.getColumnName(),
                    colStat.getNumUniqueValues(), colStat.getNumNullValues(),
                    colStat.getMinValue(), colStat.getMaxValue());
            }
        }
        catch (IOException e) {
            throw new ExecutionException(e);
        }
    }
}
