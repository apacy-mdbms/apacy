package com.apacy.queryprocessor.execution;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.apacy.common.dto.Row;
import com.apacy.common.dto.plan.SortNode;

public class SortOperator implements Operator {
    private final Operator child;
    private final SortNode node;
    private Iterator<Row> iterator;
    private List<Row> buffer;

    public SortOperator(Operator child, SortNode node) {
        this.child = child;
        this.node = node;
    }

    @Override
    public void open() {
        child.open();
        buffer = new ArrayList<>();
        Row row;
        while ((row = child.next()) != null) {
            buffer.add(row);
        }
        child.close();

        // Perform Sort using existing Strategy logic
        // SortStrategy.sort returns a new list, so we assign it
        buffer = SortStrategy.sort(buffer, node.sortColumn(), node.ascending());
        iterator = buffer.iterator();
    }

    @Override
    public Row next() {
        if (iterator != null && iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

    @Override
    public void close() {
        buffer = null;
        iterator = null;
    }
}
