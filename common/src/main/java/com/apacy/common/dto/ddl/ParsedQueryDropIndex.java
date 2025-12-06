package com.apacy.common.dto.ddl;

import com.apacy.common.enums.DDLType;

public class ParsedQueryDropIndex extends ParsedQueryDDL {
    private final String indexName;

    public ParsedQueryDropIndex(String indexName) {
        super(DDLType.DROP_INDEX);
        this.indexName = indexName;
    }

    public String getIndexName() { return indexName; }
}