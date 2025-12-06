package com.apacy.common.dto.ddl;

import com.apacy.common.enums.DDLType;

public class ParsedQueryCreateIndex extends ParsedQueryDDL {
    private final String indexName;
    private final String columnName;
    private final String indexType;

    public ParsedQueryCreateIndex(String tableName, String indexName, String columnName, String indexType) {
        super(tableName, DDLType.CREATE_INDEX);
        this.indexName = indexName;
        this.columnName = columnName;
        this.indexType = indexType;
    }

    public String getIndexName() { return indexName; }
    public String getColumnName() { return columnName; }
    public String getIndexType() { return indexType; }
}