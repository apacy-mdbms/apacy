package com.apacy.common.dto.ddl;

import com.apacy.common.enums.DDLType;

/**
 * Abstract Base Class untuk semua query DDL.
 * 'ParsedQuery' khusus untuk DDL.
 */
public abstract class ParsedQueryDDL {
    
    private final String tableName;
    private final DDLType type;

    public ParsedQueryDDL(String tableName, DDLType type) {
        this.tableName = tableName;
        this.type = type;
    }

    public String getTableName() {
        return tableName;
    }

    public DDLType getType() {
        return type;
    }
}