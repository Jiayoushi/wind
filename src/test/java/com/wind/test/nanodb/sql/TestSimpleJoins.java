package com.wind.test.nanodb.sql;



import com.wind.nanodb.expressions.TupleLiteral;
import com.wind.nanodb.relations.Tuple;
import com.wind.nanodb.server.CommandResult;
import org.junit.Test;

import java.util.ArrayList;


/**
 * This class exercises the database with some simple <tt>SELECT</tt>
 * statements against a single table, to see if simple selects and
 * predicates work properly.
 */
public class TestSimpleJoins extends SqlTestCase {

    public TestSimpleJoins() {
        super("setup_testSimpleJoins");
    }

    private TupleLiteral[] table1 = {
            new TupleLiteral(0, null, 2),
            new TupleLiteral(1, 10, 4),
            new TupleLiteral(2, 20, 7),
            new TupleLiteral(3, 30, 1),
            new TupleLiteral(4, null, 5),
            new TupleLiteral(7, 20, 5),
    };

    private TupleLiteral[] table2 = {
            new TupleLiteral(0, null, 6),
            new TupleLiteral(3, 5, 8),
            new TupleLiteral(4, 15, 10),
            new TupleLiteral(7, 20, 2),
            new TupleLiteral(5, null, 3),
            new TupleLiteral(100, 30, 4),
            new TupleLiteral(2, 20, 4),
    };

    /**
     * This test performs a simple <tt>SELECT</tt> statement with no predicate,
     * to see if the query produces the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    @Test
    public void testSelectNoPredicate() throws Throwable {
        TupleLiteral[] expected1 = table1;
        CommandResult result = server.doCommand(
                "SELECT * FROM test_simple_joins_1", true);
        assert checkUnorderedResults(expected1, result);

        TupleLiteral[] expected2 = table2;
        CommandResult result2 = server.doCommand(
                "SELECT * FROM test_simple_joins_2", true);
        assert checkUnorderedResults(expected2, result2);
    }

    /**
     * This test performs a simple Join statement with no ON clause,
     * to see if the query produces the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    @Test
    public void testCrossJoin() throws Throwable {
        TupleLiteral[] expected = new TupleLiteral[table1.length * table2.length];

        for (int i = 0; i < table1.length; i++) {
            for (int j = 0; j < table2.length; j++) {
                TupleLiteral tuple = new TupleLiteral();
                tuple.appendTuple(table1[i]);
                tuple.appendTuple(table2[j]);
                expected[i * table2.length + j] = tuple;
            }
        }

        CommandResult result = server.doCommand(
            "SELECT * FROM test_simple_joins_1, test_simple_joins_2", true);
        assert checkUnorderedResults(expected, result);
    }

    @Test
    public void testInnerJoinWithoutOnClause() throws Throwable {
        TupleLiteral[] expected = new TupleLiteral[table1.length * table2.length];

        for (int i = 0; i < table1.length; i++) {
            for (int j = 0; j < table2.length; j++) {
                TupleLiteral tuple = new TupleLiteral();
                tuple.appendTuple(table1[i]);
                tuple.appendTuple(table2[j]);
                expected[i * table2.length + j] = tuple;
            }
        }

        CommandResult result = server.doCommand(
                "SELECT * FROM test_simple_joins_1 JOIN test_simple_joins_2",true);
        assert checkUnorderedResults(expected, result);
    }

    /**
     * This test performs a simple Join statement with an ON clause,
     * to see if the query produces the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    @Test
    public void testInnerJoinWithOnClause() throws Throwable {
        ArrayList<TupleLiteral> arr = new ArrayList<>();
        for (int i = 0; i < table1.length; ++i) {
            for (int j = 0; j < table2.length; ++j) {
                if (table1[i].getColumnValue(1) != null &&
                        table1[i].getColumnValue(1) == table2[j].getColumnValue(1)) {
                    TupleLiteral tuple = new TupleLiteral();
                    tuple.appendTuple(table1[i]);
                    tuple.appendTuple(table2[j]);
                    arr.add(tuple);
                }
            }
        }

        TupleLiteral[] expected = new TupleLiteral[arr.size()];
        for (int i = 0; i < arr.size(); ++i) {
            expected[i] = arr.get(i);
        }

        CommandResult result = server.doCommand(
                "SELECT * FROM test_simple_joins_1 JOIN test_simple_joins_2" +
                        " ON test_simple_joins_1.b = test_simple_joins_2.b", true);
        assert checkUnorderedResults(expected, result);
    }

    @Test
    public void testLeftOuterJoin() throws Throwable {
        TupleLiteral[] expected = {
                new TupleLiteral(0, null, 2, null),
                new TupleLiteral(1, 10, 4, null),
                new TupleLiteral(2, 20, 7, 4),
                new TupleLiteral(3, 30, 1, null),
                new TupleLiteral(4, null, 5, null),
                new TupleLiteral(7, 20, 5, 2),
        };

        CommandResult result = server.doCommand(
                "SELECT * FROM test_simple_joins_1 LEFT OUTER JOIN test_simple_joins_2", true);
        assert checkUnorderedResults(expected, result);
    }

    @Test
    public void testRightOuterJoin() throws Throwable {
        TupleLiteral[] expected = {
                new TupleLiteral(0, null, 6, null),
                new TupleLiteral(3, 5, 8, null),
                new TupleLiteral(4, 15, 10, null),
                new TupleLiteral(7, 20, 2, 5),
                new TupleLiteral(5, null, 3, null),
                new TupleLiteral(100, 30, 4, null),
                new TupleLiteral(2, 20, 4, 7),
        };

        CommandResult result = server.doCommand(
                "SELECT * FROM test_simple_joins_1 RIGHT OUTER JOIN test_simple_joins_2", true);
        assert checkUnorderedResults(expected, result);
    }


}
