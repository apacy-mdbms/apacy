package com.apacy.queryprocessor.execution;

import java.util.HashMap;
import java.util.Map;

import com.apacy.common.dto.Row;

public class CartesianOperator implements Operator {
    private final Operator leftChild;
    private final Operator rightChild;
    
    private Row currentLeftRow;
    
    public CartesianOperator(Operator leftChild, Operator rightChild) {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
    }

    @Override
    public void open() {
        leftChild.open();
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
                // Right child exhausted. Reset.
                rightChild.close();
                currentLeftRow = leftChild.next();
                
                if (currentLeftRow == null) {
                    return null;
                }
                
                rightChild.open();
                continue;
            }
            
            return mergeRows(currentLeftRow, rightRow);
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
        for (Map.Entry<String, Object> entry : rightRow.data().entrySet()) {
            mergedData.putIfAbsent(entry.getKey(), entry.getValue());
        }
        return new Row(mergedData);
    }
}
