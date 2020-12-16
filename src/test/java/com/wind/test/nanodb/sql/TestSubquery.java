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
public class TestSubquery extends SqlTestCase {

    public TestSubquery() {
        super("setup_testSubquery");
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
                "SELECT * FROM test_subquery_1", true);
        assert checkUnorderedResults(expected1, result);

        TupleLiteral[] expected2 = table2;
        CommandResult result2 = server.doCommand(
                "SELECT * FROM test_subquery_2", true);
        assert checkUnorderedResults(expected2, result2);
    }

    @Test
    public void testSubqueryInFromClause() throws Throwable {
        TupleLiteral[] expected1 = table1;
        CommandResult result1 = server.doCommand(
                "SELECT * FROM (SELECT * FROM test_subquery_1) as table1", true);
        assert checkUnorderedResults(expected1, result1);
    }

    @Test
    public void testSubqueryInFromCaluseWithWhereAndOrderByClause() throws Throwable {
        ArrayList<TupleLiteral> list = new ArrayList<>();
        for (int i = 0; i < table1.length; i++) {
            Object value = table1[i].getColumnValue(1);
            if (value != null && (Integer)value != 30) {
                TupleLiteral tuple = new TupleLiteral();
                tuple.appendTuple(table1[i]);
                list.add(tuple);
            }
        }

        CommandResult result1 = server.doCommand(
                "SELECT * FROM " +
                        "(SELECT * FROM test_subquery_1 WHERE b != 30 ORDER BY a) as table1", true);
        assert checkUnorderedResults(tupleListToArray(list), result1);
    }

    @Test
    public void testSubqueryInFromCaluseWithTwoSubqueries() throws Throwable {
        ArrayList<TupleLiteral> list1 = new ArrayList<>();
        for (int i = 0; i < table1.length; i++) {
            Object value = table1[i].getColumnValue(1);
            if (value != null && (Integer) value != 30) {
                list1.add(table1[i]);
            }
        }
        Collections.sort(list1, new Comparator<TupleLiteral>() {
            @Override
            public int compare(TupleLiteral t1, TupleLiteral t2) {
                return compareTuple(t1, t2, 0);
            }
        });

        ArrayList<TupleLiteral> list2 = new ArrayList<>();
        for (int i = 0; i < table2.length; i++) {
            Object value = table2[i].getColumnValue(2);
            if (value != null && (Integer) value != 20) {
                list2.add(table2[i]);
            }
        }
        Collections.sort(list2, new Comparator<TupleLiteral>() {
            @Override
            public int compare(TupleLiteral t1, TupleLiteral t2) {
                return compareTuple(t1, t2, 1);
            }
        });

        ArrayList<TupleLiteral> list = new ArrayList<>();
        for (TupleLiteral t1: list1) {
            for (TupleLiteral t2: list2) {
                TupleLiteral tuple = new TupleLiteral();
                tuple.appendTuple(t1);
                tuple.appendTuple(t2);
                list.add(tuple);
            }
        }

        CommandResult result1 = server.doCommand(
                "SELECT * FROM " +
                        "(SELECT * FROM test_subquery_1 WHERE b != 30 ORDER BY a) as table1, " +
                        "(SELECT * FROM test_subquery_2 WHERE d != 20 ORDER BY b) as table2", true);
        assert checkUnorderedResults(tupleListToArray(list), result1);
    }
}
