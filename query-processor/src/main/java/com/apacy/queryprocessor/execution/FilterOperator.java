package com.apacy.queryprocessor.execution;

import com.apacy.common.dto.Row;
import com.apacy.common.dto.plan.FilterNode;
import com.apacy.queryprocessor.evaluator.ConditionEvaluator;

public class FilterOperator implements Operator {
    private final Operator child;
    private final Object predicate;

    public FilterOperator(Operator child, Object predicate) {
        this.child = child;
        this.predicate = predicate;
    }

    @Override
    public void open() {
        if (child == null) {
            throw new RuntimeException("FilterOperator: child operator is null. PlanNode might be missing a child.");
        }
        child.open();
    }

    @Override
    public Row next() {
        Row row;
        while ((row = child.next()) != null) {
            if (ConditionEvaluator.evaluate(row, predicate)) {
                return row;
            }
        }
        return null;
    }

    @Override
    public void close() {
        child.close();
    }
}
