package com.wind.test.nanodb.functions;

import com.wind.nanodb.expressions.TupleLiteral;
import com.wind.nanodb.server.CommandResult;

import com.wind.test.nanodb.sql.SqlTestCase;
import org.junit.Test;


/**
 * This class tests various functions to ensure that they work correctly.
 */
public class TestSimpleFunctions extends SqlTestCase {

    public void testAbs() throws Exception {
        CommandResult result = server.doCommand(
            "SELECT ABS(-5), ABS(3), ABS(-2.5), ABS(7.25), ABS(NULL)", true);

        TupleLiteral[] expected = {
            new TupleLiteral(5, 3, 2.5, 7.25, null)
        };

        assert checkOrderedResults(expected, result);
    }


    public void testCoalesce() throws Exception {
        CommandResult result = server.doCommand(
            "SELECT COALESCE(1), COALESCE(NULL, 2), COALESCE(NULL, 3, NULL), " +
            "COALESCE(NULL, NULL, NULL, 4), COALESCE(NULL)", true);

        TupleLiteral[] expected = {
            new TupleLiteral(1, 2, 3, 4, null)
        };

        assert checkOrderedResults(expected, result);
    }

    public void testConcat() throws Exception {
        CommandResult result = server.doCommand(
            "SELECT CONCAT('a', 'b'), CONCAT('a', 'b', 'c'), CONCAT('a', 'b', 'c', 'd'), " +
            "CONCAT(NULL, 'a'), CONCAT('a', NULL), CONCAT(NULL, NULL)", true);

        TupleLiteral[] expected = {
            new TupleLiteral("ab", "abc", "abcd", null, null, null)
        };

        assert checkOrderedResults(expected, result);
    }

}
