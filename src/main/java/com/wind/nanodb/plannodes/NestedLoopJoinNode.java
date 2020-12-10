package com.wind.nanodb.plannodes;


import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.wind.nanodb.expressions.Expression;
import com.wind.nanodb.expressions.OrderByExpression;
import com.wind.nanodb.relations.JoinType;
import com.wind.nanodb.relations.Tuple;


/**
 * This plan node implements a nested-loop join operation, which can support
 * arbitrary join conditions but is also the slowest join implementation.
 */
public class NestedLoopJoinNode extends ThetaJoinNode {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(NestedLoopJoinNode.class);


    /** Most recently retrieved tuple of the left relation. */
    private Tuple outerTuple;

    /** Most recently retrieved tuple of the right relation. */
    private Tuple innerTuple;


    /** Set to true when we have exhausted all tuples from our subplans. */
    private boolean done;

    boolean matched;
    private Tuple prevOuterTuple;

    public NestedLoopJoinNode(PlanNode leftChild, PlanNode rightChild,
                JoinType joinType, Expression predicate) {
        super(leftChild, rightChild, joinType, predicate);
    }


    /**
     * Checks if the argument is a plan node tree with the same structure, but not
     * necessarily the same references.
     *
     * @param obj the object to which we are comparing
     */
    @Override
    public boolean equals(Object obj) {

        if (obj instanceof NestedLoopJoinNode) {
            NestedLoopJoinNode other = (NestedLoopJoinNode) obj;

            return predicate.equals(other.predicate) &&
                leftChild.equals(other.leftChild) &&
                rightChild.equals(other.rightChild);
        }

        return false;
    }


    /** Computes the hash-code of the nested-loop plan node. */
    @Override
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + (predicate != null ? predicate.hashCode() : 0);
        hash = 31 * hash + leftChild.hashCode();
        hash = 31 * hash + rightChild.hashCode();
        return hash;
    }


    /**
     * Returns a string representing this nested-loop join's vital information.
     *
     * @return a string representing this plan-node.
     */
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append("NestedLoop[");

        if (predicate != null)
            buf.append("pred:  ").append(predicate);
        else
            buf.append("no pred");

        if (schemaSwapped)
            buf.append(" (schema swapped)");

        buf.append(']');

        return buf.toString();
    }


    /**
     * Creates a copy of this plan node and its subtrees.
     */
    @Override
    protected PlanNode clone() throws CloneNotSupportedException {
        NestedLoopJoinNode node = (NestedLoopJoinNode) super.clone();

        // Clone the predicate.
        if (predicate != null)
            node.predicate = predicate.duplicate();
        else
            node.predicate = null;

        return node;
    }


    /**
     * Nested-loop joins can conceivably produce sorted results in situations
     * where the outer relation is ordered, but we will keep it simple and just
     * report that the results are not ordered.
     */
    @Override
    public List<OrderByExpression> resultsOrderedBy() {
        return null;
    }


    /** True if the node supports position marking. **/
    public boolean supportsMarking() {
        return leftChild.supportsMarking() && rightChild.supportsMarking();
    }


    /** True if the node requires that its left child supports marking. */
    public boolean requiresLeftMarking() {
        return false;
    }


    /** True if the node requires that its right child supports marking. */
    public boolean requiresRightMarking() {
        return false;
    }


    @Override
    public void prepare() {
        // Need to prepare the left and right child-nodes before we can do
        // our own work.
        leftChild.prepare();
        rightChild.prepare();

        // Use the parent class' helper-function to prepare the schema.
        prepareSchemaStats();

        // TODO:  Implement the rest
        cost = null;
    }


    public void initialize() {
        super.initialize();

        done = false;
        matched = false;
        prevOuterTuple = null;
        outerTuple = null;
        innerTuple = null;
    }

    /**
     * Returns the next joined tuple that satisfies the join condition.
     *
     * @return the next joined tuple that satisfies the join condition.
     *
     * @throws IOException if a db file failed to open at some point
     */
    public Tuple getNextTuple() throws IOException {
        if (done)
            return null;

        while (getTuplesToJoin()) {
            if (innerTuple == null) {
                if (isOuterJoin() && !matched) {
                    matched = false;
                    return joinTuplesPadNull(prevOuterTuple);
                } else {
                    matched = false;
                    continue;
                }
            }

            if (canJoinTuples()) {
                matched = true;
                return joinTuples(outerTuple, innerTuple);
            }
        }

        return null;
    }


    /**
     * This helper function implements the logic that sets {@link #outerTuple}
     * and {@link #innerTuple} based on the nested-loop logic.
     *
     * @return {@code true} if another pair of tuples was found to join, or
     *         {@code false} if no more pairs of tuples are available to join.
     */
    private boolean getTuplesToJoin() throws IOException {
        if (outerTuple == null && (outerTuple = getNextOuterTuple()) == null) {
            done = true;
            return false;
        }

        innerTuple = getNextInnerTuple();
        if (innerTuple == null) {
            prevOuterTuple = outerTuple;
            if ((outerTuple = getNextOuterTuple()) == null) {
                done = true;
                return false;
            }
            initializeInnerTuple();
            return true;
        }

        return true;
    }


    private boolean canJoinTuples() {
        if (isOuterJoin()) {
            if (predicate == null) {
                return hasEqualColumnsAndEqualValues(innerTuple, outerTuple);
            }
        } else {
            if (predicate == null)
                return true;
        }

        environment.clear();
        environment.addTuple(leftSchema, outerTuple);
        environment.addTuple(rightSchema, innerTuple);

        return predicate.evaluatePredicate(environment);
    }


    public void markCurrentPosition() {
        leftChild.markCurrentPosition();
        rightChild.markCurrentPosition();
    }


    public void resetToLastMark() throws IllegalStateException {
        leftChild.resetToLastMark();
        rightChild.resetToLastMark();

        // TODO:  Prepare to reevaluate the join operation for the tuples.
        //        (Just haven't gotten around to implementing this.)
    }

    private Tuple getNextInnerTuple() throws IOException {
        if (joinType == JoinType.LEFT_OUTER) {
            return rightChild.getNextTuple();
        } else if (joinType == JoinType.RIGHT_OUTER) {
            return leftChild.getNextTuple();
        } else {
            return rightChild.getNextTuple();
        }
    }

    private Tuple getNextOuterTuple() throws IOException {
        if (joinType == JoinType.LEFT_OUTER) {
            return leftChild.getNextTuple();
        } else if (joinType == JoinType.RIGHT_OUTER) {
            return rightChild.getNextTuple();
        } else {
            return leftChild.getNextTuple();
        }
    }

    private void initializeInnerTuple() {
        if (joinType == JoinType.LEFT_OUTER) {
            rightChild.initialize();
        } else if (joinType == JoinType.RIGHT_OUTER) {
            leftChild.initialize();
        } else {
            rightChild.initialize();
        }
    }

    public void cleanUp() {
        leftChild.cleanUp();
        rightChild.cleanUp();
    }
}
