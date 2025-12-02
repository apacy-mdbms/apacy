package com.apacy.queryprocessor.execution;

import com.apacy.common.dto.Row;

public interface Operator {
    /**
     * Initializes the operator. Allocates resources, opens files, etc.
     */
    void open();

    /**
     * Returns the next row from the operator.
     * Returns null if there are no more rows.
     */
    Row next();

    /**
     * Closes the operator. Releases resources.
     */
    void close();
}
