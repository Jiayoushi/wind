package com.wind.nanodb.plannodes;


import com.wind.nanodb.expressions.Expression;
import com.wind.nanodb.expressions.TupleLiteral;

import com.wind.nanodb.queryeval.ColumnStats;
import com.wind.nanodb.relations.JoinType;
import com.wind.nanodb.relations.Schema;
import com.wind.nanodb.relations.Tuple;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;


/**
 * PlanNode representing the <tt>FROM</tt> clause in a <tt>SELECT</tt>
 * operation.  This is the relational algebra ThetaJoin operator.
 */
public abstract class ThetaJoinNode extends PlanNode {

    /** The type of the join operation to perform. */
    public JoinType joinType;


    /** Join condition. */
    public Expression predicate;


    /**
     * The cached schema of the left subplan, used for join-predicate
     * evaluation.
     */
    protected Schema leftSchema;


    /** The cached statistics of the left subplan, used for cost estimation. */
    protected ArrayList<ColumnStats> leftStats;


    /**
     * The cached schema of the right subplan, used for join-predicate
     * evaluation.
     */
    protected Schema rightSchema;


    /** The cached statistics of the right subplan, used for cost estimation. */
    protected ArrayList<ColumnStats> rightStats;


    /**
     * True if the output schema of this node is swapped.  If this flag is true
     * then tuples from the left subplan will be on the right of joined results
     * and so forth, but if the flag is false then the tuples from the left
     * subplan will be on the left of the joined results.  The schema and the
     * statistics values reflect the state of this flag, after {@link #prepare}
     * has been called.
     */
    protected boolean schemaSwapped = false;


    /**
     * Constructs a ThetaJoinNode that joins the tuples from the left and right
     * subplans, using the specified join type and join predicate.
     *
     * @param leftChild the left relation
     *
     * @param rightChild the right relation
     *
     * @param joinType the type of join operation to perform
     *
     * @param predicate the join condition
     */
    public ThetaJoinNode(PlanNode leftChild, PlanNode rightChild,
        JoinType joinType, Expression predicate) {

        super(OperationType.THETA_JOIN, leftChild, rightChild);

        if (joinType == null)
            throw new IllegalArgumentException("joinType cannot be null");

        this.joinType = joinType;
        this.predicate = predicate;
    }


    /**
     * Combine the left tuple and the right tuple. If schemaSwapped is set to
     * true, the tuples are copied in the opposite order.  This can only
     * happen if swap() was called an odd number of times, switching the left
     * and right subtrees.
     *
     * @param left the left tuple
     * @param right the right tuple
     * @return the combined tuple
     */
    protected Tuple joinTuples(Tuple left, Tuple right) {

        // TODO:  Extend this to support semi-join and anti-join.
        if (joinType == JoinType.SEMIJOIN || joinType == JoinType.ANTIJOIN)
            throw new UnsupportedOperationException("Not yet implemented!");

        TupleLiteral joinedTuple = new TupleLiteral();
        // appendTuple() also copies schema information from the source tuples.

        if (!schemaSwapped) {
            joinedTuple.appendTuple(left);
            joinedTuple.appendTuple(right);
        }
        else {
            joinedTuple.appendTuple(right);
            joinedTuple.appendTuple(left);
        }

        return joinedTuple;
    }

    protected boolean isOuterJoin() {
        return joinType == JoinType.LEFT_OUTER || joinType == JoinType.RIGHT_OUTER;
    }

    protected boolean hasEqualColumnsAndEqualValues(Tuple left, Tuple right) {
        Set<String> commonColumnNames = leftSchema.getCommonColumnNames(rightSchema);
        if (commonColumnNames.size() == 0) {
            return false;
        }
        for (String columnName : commonColumnNames) {
            int leftIndex = leftSchema.getColumnIndex(columnName);
            int rightIndex = rightSchema.getColumnIndex(columnName);
            Object leftValue = left.getColumnValue(leftIndex);
            Object rightValue = right.getColumnValue(rightIndex);

            if (leftValue == null || rightValue == null || !leftValue.equals(rightValue)) {
                return false;
            }
        }
        return true;
    }

    protected Tuple joinTuplesPadNull(Tuple left, int nullCount) {
        //System.out.println(String.format("[%s] [%s] [%b]\n", left.toString(), right.toString(), abortIfNotEqual));

        TupleLiteral joinedTuple = new TupleLiteral();
        joinedTuple.appendTuple(left);

        for (int i = 0; i < nullCount; ++i) {
            joinedTuple.addValue(null);
        }
        return joinedTuple;
    }

    /**
     * Do initialization for the join operation. Resets state variables.
     * Initialize both children.
     */
    public void initialize() {
        super.initialize();

        if (joinType != JoinType.CROSS && joinType != JoinType.INNER
                && joinType != JoinType.LEFT_OUTER && joinType != JoinType.RIGHT_OUTER) {
            throw new UnsupportedOperationException(
                "We don't support joins of type " + joinType + " yet!");
        }

        leftChild.initialize();
        rightChild.initialize();
    }


    /**
     * This helper method can be used by the {@link #prepare} method in
     * subclasses, to compute the output schema and initial stats of the
     * join operation.  This method is provided because it takes the
     * {@link #schemaSwapped} flag into account when ordering the schema
     * and stats.
     */
    protected void prepareSchemaStats() {
        leftSchema = leftChild.getSchema();
        rightSchema = rightChild.getSchema();

        leftStats = leftChild.getStats();
        rightStats = rightChild.getStats();

        stats = new ArrayList<ColumnStats>();

        schema = new Schema();
        if (!schemaSwapped) {
            schema.append(leftSchema);
            schema.append(rightSchema);

            stats.addAll(leftStats);
            stats.addAll(rightStats);
        }
        else {
            schema.append(rightSchema);
            schema.append(leftSchema);

            stats.addAll(rightStats);
            stats.addAll(leftStats);
        }
    }


    /**
     * Swaps the left child and right child subtrees. Ensures that the schema
     * of the node does not change in the swap, so that this is still a valid
     * query plan.
     */
    public void swap() {
        PlanNode left = leftChild;
        leftChild = rightChild;
        rightChild = left;

        schemaSwapped = !schemaSwapped;
    }


    /**
     * Returns true if the schema is swapped in this theta join node, false
     * otherwise.
     *
     * @return true if the schema is swapped in this theta join node, false
     *         otherwise.
     */
    public boolean isSwapped() {
        return schemaSwapped;
    }
}
