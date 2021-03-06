package com.wind.nanodb.queryeval;


import com.wind.nanodb.expressions.*;
import com.wind.nanodb.plannodes.*;
import com.wind.nanodb.queryast.FromClause;
import com.wind.nanodb.queryast.SelectClause;
import com.wind.nanodb.queryast.SelectValue;
import com.wind.nanodb.relations.JoinType;
import com.wind.nanodb.relations.TableInfo;
import com.wind.nanodb.storage.PageTuple;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;


/**
 * This class generates execution plans for very simple SQL
 * <tt>SELECT * FROM tbl [WHERE P]</tt> queries.  The primary responsibility
 * is to generate plans for SQL <tt>SELECT</tt> statements, but
 * <tt>UPDATE</tt> and <tt>DELETE</tt> expressions will also use this class
 * to generate simple plans to identify the tuples to update or delete.
 */
public class SimplePlanner extends AbstractPlannerImpl {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(SimplePlanner.class);


    /**
     * Returns the root of a plan tree suitable for executing the specified
     * query.
     *
     * @param selectClause an object describing the query to be performed
     *
     * @return a plan tree for executing the specified query
     *
     * @throws IOException if an IO error occurs when the planner attempts to
     *         load schema and indexing information.
     */
    @Override
    public PlanNode makePlan(SelectClause selectClause,
                             List<SelectClause> enclosingSelects) throws IOException {
        if (enclosingSelects != null && !enclosingSelects.isEmpty()) {
            throw new UnsupportedOperationException(
                "Not implemented:  enclosing queries");
        }

        PlanNode planNode = null;

        AggregateProcessor aggregateProcessor = new AggregateProcessor();
        for (SelectValue sv: selectClause.getSelectValues()) {
            // Skip select-values that aren't expressions
            if (!sv.isExpression()) {
                continue;
            }
            Expression exp = sv.getExpression().traverse(aggregateProcessor);
            sv.setExpression(exp);
        }

        // From
        FromClause fromClause = selectClause.getFromClause();
        planNode = generateFromClausePlan(fromClause);

        // Where
        if (selectClause.getWhereExpr() != null) {
            planNode = PlanUtils.addPredicateToPlan(planNode, selectClause.getWhereExpr());
        }

        // Group by and aggregation
        if (!selectClause.getGroupByExprs().isEmpty() || aggregateProcessor.hasAggregates()) {
            planNode = handleGroupingAndAggregation(planNode, selectClause, aggregateProcessor);
        }

        // Order by
        if (selectClause.getOrderByExprs().size() != 0) {
            planNode = new SortNode(planNode, selectClause.getOrderByExprs());
        }

        // Project
        if (!selectClause.isTrivialProject()) {
            planNode = new ProjectNode(planNode, selectClause.getSelectValues());
        }

        planNode.prepare();
        return planNode;
    }


    private PlanNode handleGroupingAndAggregation(PlanNode planNode, SelectClause selectClause,
                                                  AggregateProcessor processor) {
        /**
         * The SELECT and HAVING expressions must be scanned for aggregate expressions so that the grouping plan-node
         * can be initialized correctly, and also so that the aggregates may be removed and replaced with
         * placeholder variables.
         */
        return new HashedGroupAggregateNode(planNode, selectClause.getGroupByExprs(), processor.getAggregates());
    }

    private PlanNode generateFromClausePlan(FromClause fromClause) throws IOException {
        PlanNode planNode = null;

        if (fromClause.isBaseTable()) {
            TableInfo tableInfo = storageManager.getTableManager().openTable(fromClause.getTableName());
            planNode = new FileScanNode(tableInfo, null);
        } else if (fromClause.getClauseType() == FromClause.ClauseType.JOIN_EXPR) {
            planNode = new NestedLoopJoinNode(generateFromClausePlan(fromClause.getLeftChild()),
                    generateFromClausePlan(fromClause.getRightChild()),
                    fromClause.getJoinType(), fromClause.getOnExpression());
        } else if (fromClause.getClauseType() == FromClause.ClauseType.SELECT_SUBQUERY) {
            planNode = makePlan(fromClause.getSelectClause(), null);
        } else {
            throw new UnsupportedOperationException("Not implemented");
        }

        if (fromClause.isRenamed()) {
            planNode = new RenameNode(planNode, fromClause.getResultName());
        }

        return planNode;
    }

    /**
     * Constructs a simple select plan that reads directly from a table, with
     * an optional predicate for selecting rows.
     * <p>
     * While this method can be used for building up larger <tt>SELECT</tt>
     * queries, the returned plan is also suitable for use in <tt>UPDATE</tt>
     * and <tt>DELETE</tt> command evaluation.  In these cases, the plan must
     * only generate tuples of type {@link PageTuple},
     * so that the command can modify or delete the actual tuple in the file's
     * page data.
     *
     * @param tableName The name of the table that is being selected from.
     *
     * @param predicate An optional selection predicate, or {@code null} if
     *        no filtering is desired.
     *
     * @return A new plan-node for evaluating the select operation.
     *
     * @throws IOException if an error occurs when loading necessary table
     *         information.
     */
    public SelectNode makeSimpleSelect(String tableName, Expression predicate,
                                       List<SelectClause> enclosingSelects) throws IOException {
        if (tableName == null)
            throw new IllegalArgumentException("tableName cannot be null");

        if (enclosingSelects != null) {
            // If there are enclosing selects, this subquery's predicate may
            // reference an outer query's value, but we don't detect that here.
            // Therefore we will probably fail with an unrecognized column
            // reference.
            logger.warn("Currently we are not clever enough to detect " +
                "correlated subqueries, so expect things are about to break...");
        }

        // Open the table.
        TableInfo tableInfo = storageManager.getTableManager().openTable(tableName);

        // Make a SelectNode to read rows from the table, with the specified
        // predicate.
        SelectNode selectNode = new FileScanNode(tableInfo, predicate);
        selectNode.prepare();
        return selectNode;
    }
}

