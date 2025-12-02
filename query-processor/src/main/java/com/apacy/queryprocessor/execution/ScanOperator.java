package com.apacy.queryprocessor.execution;

import java.util.Iterator;
import java.util.List;

import com.apacy.common.dto.DataRetrieval;
import com.apacy.common.dto.Row;
import com.apacy.common.dto.plan.ScanNode;
import com.apacy.common.interfaces.IConcurrencyControlManager;
import com.apacy.common.interfaces.IStorageManager;

public class ScanOperator implements Operator {
    private final ScanNode node;
    private final IStorageManager sm;
    private Iterator<Row> iterator;
    private List<Row> buffer;

    public ScanOperator(ScanNode node, IStorageManager sm) {
        this.node = node;
        this.sm = sm;
    }

    @Override
    public void open() {
        // In a real implementation, this would open a cursor/file stream.
        // Due to StorageManager limitations, we read the block into memory here.
        DataRetrieval dr = new DataRetrieval(
            node.tableName(), 
            null, 
            null, 
            false
        );
        this.buffer = sm.readBlock(dr);
        if (this.buffer != null) {
            this.iterator = this.buffer.iterator();
        }
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
        this.buffer = null;
        this.iterator = null;
    }
}
