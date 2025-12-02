package com.apacy.queryprocessor.execution;

import java.util.HashMap;
import java.util.Map;

import com.apacy.common.dto.Row;
import com.apacy.queryprocessor.evaluator.ConditionEvaluator;

public class NestedLoopJoinOperator implements Operator {
    private final Operator leftChild;
    private final Operator rightChild;
    private final Object joinCondition;
    
    private Row currentLeftRow;
    
    public NestedLoopJoinOperator(Operator leftChild, Operator rightChild, Object joinCondition) {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.joinCondition = joinCondition;
    }

    @Override
    public void open() {
        leftChild.open();
        // We don't open rightChild here immediately; we open it when we have a left row.
        // Or we open it and then close/open in the loop.
        currentLeftRow = leftChild.next();
        if (currentLeftRow != null) {
            rightChild.open();
        }
    }

    @Override
    public Row next() {
        while (currentLeftRow != null) {
            Row rightRow = rightChild.next();
            
            if (rightRow == null) {
                // Right child exhausted. Reset right child and advance left.
                rightChild.close();
                currentLeftRow = leftChild.next();
                
                if (currentLeftRow == null) {
                    return null; // Both exhausted
                }
                
                rightChild.open();
                continue; // Restart loop with new left row and fresh right stream
            }
            
            // Check join condition
            // Create merged row candidate
            Row mergedRow = mergeRows(currentLeftRow, rightRow);
            
            if (ConditionEvaluator.evaluate(mergedRow, joinCondition)) {
                return mergedRow;
            }
        }
        return null;
    }

    @Override
    public void close() {
        leftChild.close();
        rightChild.close();
    }

    private Row mergeRows(Row leftRow, Row rightRow) {
        Map<String, Object> mergedData = new HashMap<>(leftRow.data());
        // Right row data overrides collision? or preserve? 
        // Standard SQL: columns should be distinct or prefixed. 
        // Our previous JoinStrategy logic:
        // mergedData.putIfAbsent(entry.getKey(), entry.getValue()); (Left takes precedence)
        
        for (Map.Entry<String, Object> entry : rightRow.data().entrySet()) {
            mergedData.putIfAbsent(entry.getKey(), entry.getValue());
        }
        return new Row(mergedData);
    }
}
