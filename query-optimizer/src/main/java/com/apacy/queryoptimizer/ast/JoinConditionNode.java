package com.apacy.queryoptimizer.ast;

public class JoinConditionNode {

    private String joinType; // CROSS, INNER
    private String column;
    private JoinConditionNode left;
    private JoinConditionNode right;
    private WhereConditionNode conditions;

    public JoinConditionNode(
        String joinType,
        JoinConditionNode left,
        JoinConditionNode right,
        WhereConditionNode conditions
    ) {
        this.column = null;

        this.joinType = joinType;
        this.left = left;
        this.right = right;
        this.conditions = conditions;
    }

    public JoinConditionNode(
        String column
    ) {
        this.column = column;

        this.joinType = null;
        this.left = null;
        this.right = null;
        this.conditions = null;
    }

    // Getters
    public String getJoinType() { return joinType; }
    public String getColumn() { return column; }
    public JoinConditionNode getLeft() { return left; }
    public JoinConditionNode getRight() { return right; }
    public WhereConditionNode getConditions() { return conditions; }

    public boolean isLeaf() {
        return this.column != null;
    }
}
