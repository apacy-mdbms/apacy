package com.apacy.queryprocessor.execution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.apacy.common.dto.Row;
import com.apacy.queryprocessor.evaluator.ConditionEvaluator;

public class NestedLoopJoinOperator implements Operator {
    private final Operator leftChild;
    private final Operator rightChild;
    private final Object joinCondition;
    private final String joinType; // "INNER", "LEFT"
    
    // Cache right table to avoid repetitive disk I/O
    private List<Row> rightTableCache;
    private boolean rightTableCached = false;
    
    // State variables
    private Row currentLeftRow;
    private int rightIndex;
    private boolean matchFoundForLeft;

    public NestedLoopJoinOperator(Operator leftChild, Operator rightChild, Object joinCondition, String joinType) {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.joinCondition = joinCondition;
        this.joinType = joinType != null ? joinType.toUpperCase() : "INNER";
    }

    @Override
    public void open() {
        leftChild.open();
        
        // Cache Right Table (Block-Nested Loop style)
        if (!rightTableCached) {
            rightTableCache = new ArrayList<>();
            rightChild.open();
            Row r;
            while ((r = rightChild.next()) != null) {
                rightTableCache.add(r);
            }
            rightChild.close();
            rightTableCached = true;
        }

        currentLeftRow = leftChild.next();
        rightIndex = 0;
        matchFoundForLeft = false;
    }

    @Override
    public Row next() {
        while (currentLeftRow != null) {
            
            while (rightIndex < rightTableCache.size()) {
                Row rightRow = rightTableCache.get(rightIndex);
                rightIndex++;

                Row mergedRow = mergeRows(currentLeftRow, rightRow);
                
                if (ConditionEvaluator.evaluate(mergedRow, joinCondition)) {
                    matchFoundForLeft = true;
                    return mergedRow;
                }
            }

            if (joinType.contains("LEFT") && !matchFoundForLeft) {
                Row nullRow = createNullRow(currentLeftRow);
                
                advanceLeft(); 
                return nullRow;
            }

            advanceLeft();
        }

        return null;
    }

    private void advanceLeft() {
        currentLeftRow = leftChild.next();
        rightIndex = 0;
        matchFoundForLeft = false;
    }

    @Override
    public void close() {
        leftChild.close();
        rightChild.close();
    }

    private Row mergeRows(Row left, Row right) {
        Map<String, Object> data = new HashMap<>(left.data());
        for (var entry : right.data().entrySet()) {
            data.putIfAbsent(entry.getKey(), entry.getValue());
        }
        return new Row(data);
    }

    private Row createNullRow(Row left) {
        Map<String, Object> data = new HashMap<>(left.data());
    
        if (!rightTableCache.isEmpty()) {
            Row sample = rightTableCache.get(0);
            for (String key : sample.data().keySet()) {
                data.putIfAbsent(key, null);
            }
        }
        return new Row(data);
    }
}