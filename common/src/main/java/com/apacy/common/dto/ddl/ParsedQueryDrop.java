package com.apacy.common.dto.ddl;

import com.apacy.common.enums.DDLType;

public class ParsedQueryDrop extends ParsedQueryDDL {

    private final boolean isCascading;

    public ParsedQueryDrop(String tableName, boolean isCascading) {
        super(tableName, DDLType.DROP_TABLE);
        this.isCascading = isCascading;
    }

    public boolean isCascading() {
        return isCascading;
    }
}