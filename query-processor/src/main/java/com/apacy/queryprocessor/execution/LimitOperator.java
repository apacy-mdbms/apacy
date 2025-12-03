package com.apacy.queryprocessor.execution;

import com.apacy.common.dto.Row;
import com.apacy.common.dto.plan.LimitNode;

public class LimitOperator implements Operator {
    private final Operator child;
    private final int limit;
    private final int offset;
    private int count = 0;

    public LimitOperator(Operator child, LimitNode node) {
        this.child = child;
        this.limit = node.limit();
        this.offset = 0; // LimitNode currently does not support offset
    }

    public LimitOperator(Operator child, int limit) {
        this(child, limit, 0);
    }

    public LimitOperator(Operator child, int limit, int offset) {
        this.child = child;
        this.limit = limit;
        this.offset = offset;
    }

    @Override
    public void open() {
        child.open();
        count = 0;
        
        // Handle Offset
        int skipped = 0;
        while (skipped < offset) {
            if (child.next() == null) {
                break;
            }
            skipped++;
        }
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
