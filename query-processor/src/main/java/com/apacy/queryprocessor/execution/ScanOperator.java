package com.apacy.queryprocessor.execution;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.apacy.common.dto.DataRetrieval;
import com.apacy.common.dto.Row;
import com.apacy.common.dto.ast.where.WhereConditionNode;
import com.apacy.common.dto.plan.ScanNode;
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
        boolean useIndex = (node.indexName() != null);
        WhereConditionNode filterCondition = (WhereConditionNode) node.condition();
        // In a real implementation, this would open a cursor/file stream.
        // Due to StorageManager limitations, we read the block into memory here.
        DataRetrieval dr = new DataRetrieval(
            node.tableName(), 
            null,
            filterCondition, 
            useIndex 
        );
        this.buffer = sm.readBlock(dr);
        if (this.buffer != null) {
            this.iterator = this.buffer.iterator();
        }
    }

    @Override
    public Row next() {
        if (iterator != null && iterator.hasNext()) {
            Row rawRow = iterator.next();

            System.out.println("ScanOperator DEBUG: Table=" + node.tableName() + ", Alias='" + node.alias() + "'");
            
            Map<String, Object> prefixedData = new HashMap<>();
            String prefix = (node.alias() != null && !node.alias().isEmpty()) 
                            ? node.alias() 
                            : node.tableName();

            for (Map.Entry<String, Object> entry : rawRow.data().entrySet()) {
                String newKey = prefix + "." + entry.getKey();
                prefixedData.put(newKey, entry.getValue());
            }

            return new Row(prefixedData);
        }
        return null;
    }

    @Override
    public void close() {
        this.buffer = null;
        this.iterator = null;
    }
}
