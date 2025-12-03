package com.apacy.queryprocessor.execution;

import com.apacy.common.dto.Row;
import com.apacy.common.dto.plan.LimitNode;

public class LimitOperator implements Operator {
    private final Operator child;
    private final int limit;
    private int count = 0;

    public LimitOperator(Operator child, LimitNode node) {
        this.child = child;
        this.limit = node.limit();
    }

    @Override
    public void open() {
        child.open();
        count = 0;
    }

    @Override
    public Row next() {
        if (count >= limit) {
            return null;
        }
        
        Row row = child.next();
        if (row != null) {
            count++;
            return row;
        }
        
        return null;
    }

    @Override
    public void close() {
        child.close();
    }
}
