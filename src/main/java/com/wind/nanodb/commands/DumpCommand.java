package com.wind.nanodb.commands;


import java.io.IOException;
import java.io.PrintStream;

import com.wind.nanodb.server.NanoDBServer;
import com.wind.nanodb.storage.StorageManager;
import com.wind.nanodb.plannodes.PlanNode;
import com.wind.nanodb.queryeval.EvalStats;
import com.wind.nanodb.queryeval.QueryEvaluator;
import com.wind.nanodb.queryeval.TupleProcessor;
import com.wind.nanodb.relations.ColumnInfo;
import com.wind.nanodb.relations.Schema;
import com.wind.nanodb.relations.SchemaNameException;
import com.wind.nanodb.relations.Tuple;


/**
 * An abstract base-class that holds the common implementation of the various
 * kinds of dump commands.
 */
public abstract class DumpCommand extends Command {

    /**
     * An implementation of the tuple processor interface used by the
     * {@link DumpTableCommand} to dump each tuple.
     */
    protected static class TupleExporter implements TupleProcessor {
        private PrintStream dumpOut;


        /**
         * Initialize the tuple-exporter object with the details it needs to
         * print out tuples from the specified table.
         */
        public TupleExporter(PrintStream dumpOut) {
            this.dumpOut = dumpOut;
        }


        /** The exporter can output the schema to the dump file. */
        public void setSchema(Schema schema) {
            dumpOut.print("{");

            boolean first = true;
            for (ColumnInfo colInfo : schema) {
                if (first)
                    first = false;
                else
                    dumpOut.print(",");

                String colName = colInfo.getName();
                String tblName = colInfo.getTableName();

                // TODO:  To only print out table-names when the column-name
                //        is ambiguous by itself, uncomment the first part and
                //        then comment out the next part.

                // Only print out the table name if there are multiple columns
                // with this column name.
                // if (schema.numColumnsWithName(colName) > 1 && tblName != null)
                //     out.print(tblName + '.');

                // If table name is specified, always print it out.
                if (tblName != null)
                    dumpOut.print(tblName + '.');

                dumpOut.print(colName);

                dumpOut.print(":");
                dumpOut.print(colInfo.getType());
            }
            dumpOut.println("}");
        }

        /** This implementation simply prints out each tuple it is handed. */
        public void process(Tuple tuple) throws IOException {
            dumpOut.print("[");
            boolean first = true;
            for (int i = 0; i < tuple.getColumnCount(); i++) {
                if (first)
                    first = false;
                else
                    dumpOut.print(", ");

                Object val = tuple.getColumnValue(i);
                if (val instanceof String)
                    dumpOut.printf("\"%s\"", val);
                else
                    dumpOut.print(val);
            }
            dumpOut.println("]");
        }

        public void finish() {
            // Not used
        }
    }


    /** The path and filename to dump the table data to, if desired. */
    protected String fileName;


    /** The data format to use when dumping the table data. */
    protected String format;


    protected DumpCommand(String fileName, String format) {
        super(Command.Type.UTILITY);

        this.fileName = fileName;
        this.format = format;
    }


    @Override
    public void execute(NanoDBServer server) throws ExecutionException {

        try {
            // Figure out where the dumped data should go.
            PrintStream dumpOut = out;
            if (fileName != null)
                dumpOut = new PrintStream(fileName);

            // Dump the table.
            PlanNode dumpPlan = prepareDumpPlan(server.getStorageManager());
            TupleExporter exporter = new TupleExporter(dumpOut);
            EvalStats stats = QueryEvaluator.executePlan(dumpPlan, exporter);

            if (fileName != null)
                dumpOut.close();

            // Print out the evaluation statistics.
            out.printf("Dumped %d rows in %f sec.%n",
                stats.getRowsProduced(), stats.getElapsedTimeSecs());
        }
        catch (ExecutionException e) {
            throw e;
        }
        catch (Exception e) {
            throw new ExecutionException(e);
        }
    }


    protected abstract PlanNode prepareDumpPlan(StorageManager storageManager)
        throws IOException, SchemaNameException;
}