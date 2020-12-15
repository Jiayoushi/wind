package com.wind.test.nanodb.sql;



import com.wind.nanodb.expressions.TupleLiteral;
import com.wind.nanodb.relations.Tuple;
import com.wind.nanodb.server.CommandResult;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;


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
            new TupleLiteral(2, 20, 7),
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
                        table1[i].getColumnValue(1).equals(table2[j].getColumnValue(1))) {
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

    private boolean equal(Tuple a, Tuple b, int... columnsToCheck) {
        for (int column: columnsToCheck) {
            if (a.getColumnValue(column) == null || b.getColumnValue(column) == null) {
                return false;
            }
            if (a.getColumnValue(column) != b.getColumnValue(column)) {
                return false;
            }
        }
        return true;
    }

    @Test
    public void testLeftOuterJoin() throws Throwable {
        ArrayList<TupleLiteral> arr = new ArrayList<>();
        for (int i = 0; i < table1.length; ++i) {
            boolean matched = false;
            for (int j = 0; j < table2.length; ++j) {
                if (equal(table1[i], table2[j], 0, 1)) {
                    TupleLiteral tuple = new TupleLiteral();
                    tuple.appendTuple(table1[i]);
                    tuple.appendTuple(table2[j]);
                    arr.add(tuple);
                    matched = true;
                }
            }
            if (!matched) {
                TupleLiteral tuple = new TupleLiteral(table1[i]);
                for (int x = 0; x < table2[0].getColumnCount(); x++) {
                    tuple.addValue(null);
                }
                arr.add(tuple);
            }
        }

        TupleLiteral[] expected = new TupleLiteral[arr.size()];
        for (int i = 0; i < arr.size(); ++i) {
            expected[i] = arr.get(i);
        }

        CommandResult result = server.doCommand(
                "SELECT * FROM test_simple_joins_1 LEFT OUTER JOIN test_simple_joins_2", true);
        assert checkUnorderedResults(expected, result);
    }

    @Test
    public void testRightOuterJoin() throws Throwable {
        ArrayList<TupleLiteral> arr = new ArrayList<>();
        for (int i = 0; i < table2.length; ++i) {
            boolean matched = false;
            for (int j = 0; j < table1.length; ++j) {
                if (equal(table2[i], table1[j], 0, 1)) {
                    TupleLiteral tuple = new TupleLiteral();
                    tuple.appendTuple(table2[i]);
                    tuple.appendTuple(table1[j]);
                    arr.add(tuple);
                    matched = true;
                }
            }
            if (!matched) {
                TupleLiteral tuple = new TupleLiteral(table2[i]);
                for (int x = 0; x < table1[0].getColumnCount(); x++) {
                    tuple.addValue(null);
                }
                arr.add(tuple);
            }
        }

        TupleLiteral[] expected = new TupleLiteral[arr.size()];
        for (int i = 0; i < arr.size(); ++i) {
            expected[i] = arr.get(i);
        }

        CommandResult result = server.doCommand(
                "SELECT * FROM test_simple_joins_1 RIGHT OUTER JOIN test_simple_joins_2", true);
        assert checkUnorderedResults(expected, result);
    }

    @Test
    public void testCrossJoinWithWhereClause() throws Throwable {
        ArrayList<TupleLiteral> arr = new ArrayList<>();
        for (int i = 0; i < table1.length; ++i) {
            for (int j = 0; j < table2.length; ++j) {
                if (equal(table1[i], table2[j], 2)) {
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
                "SELECT * FROM test_simple_joins_1, test_simple_joins_2 " +
                "WHERE test_simple_joins_1.c = test_simple_joins_2.d", true);
        assert checkUnorderedResults(expected, result);
    }

    @Test
    public void testRightOuterJoinWithWhereClause() throws Throwable {
        ArrayList<TupleLiteral> arr = new ArrayList<>();
        for (int i = 0; i < table1.length; ++i) {
            for (int j = 0; j < table2.length; ++j) {
                if (equal(table1[i], table2[j], 0, 1, 2)) {
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
                "SELECT * FROM test_simple_joins_1 RIGHT OUTER JOIN test_simple_joins_2 "
                + "WHERE test_simple_joins_1.c = test_simple_joins_2.d", true);
        assert checkUnorderedResults(expected, result);
    }

    @Test
    public void testLeftOuterJoinWithOrderBy() throws Throwable {
        ArrayList<TupleLiteral> arr = new ArrayList<>();
        for (int i = 0; i < table1.length; ++i) {
            boolean matched = false;
            for (int j = 0; j < table2.length; ++j) {
                if (equal(table1[i], table2[j], 0, 1)) {
                    TupleLiteral tuple = new TupleLiteral();
                    tuple.appendTuple(table1[i]);
                    tuple.appendTuple(table2[j]);
                    arr.add(tuple);
                    matched = true;
                }
            }
            if (!matched) {
                TupleLiteral tuple = new TupleLiteral(table1[i]);
                for (int x = 0; x < table2[0].getColumnCount(); x++) {
                    tuple.addValue(null);
                }
                arr.add(tuple);
            }
        }

        Collections.sort(arr, new Comparator<TupleLiteral>() {
            @Override
            public int compare(TupleLiteral t1, TupleLiteral t2) {
                Object v1 = t1.getColumnValue(0);
                Object v2 = t2.getColumnValue(0);
                if (v1 == null) {
                    return -1;
                } else if (v2 == null) {
                    return 1;
                } else {
                    return (Integer)t1.getColumnValue(0) - (Integer)t2.getColumnValue(0);
                }
            }
        });

        TupleLiteral[] expected = new TupleLiteral[arr.size()];
        for (int i = 0; i < arr.size(); ++i) {
            expected[i] = arr.get(i);
        }

        CommandResult result = server.doCommand(
                "SELECT *" +
                        " FROM test_simple_joins_1 LEFT OUTER JOIN test_simple_joins_2" +
                        " ORDER BY test_simple_joins_1.a", true);
        assert checkOrderedResults(expected, result);
    }
}
