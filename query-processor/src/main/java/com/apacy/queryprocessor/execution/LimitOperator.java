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
        this.offset = node.offset();
    }

    @Override
    public void open() {
        child.open();
        count = 0;

        int skipped = 0;
        while (skipped < offset) {
            Row discarded = child.next();
            if (discarded == null) break;
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
