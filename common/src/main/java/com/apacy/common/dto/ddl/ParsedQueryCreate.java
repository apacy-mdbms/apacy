package com.apacy.common.dto.ddl;

import java.util.ArrayList;
import java.util.List;

import com.apacy.common.dto.ForeignKeySchema;
import com.apacy.common.enums.DDLType;

public class ParsedQueryCreate extends ParsedQueryDDL {

    private final List<ColumnDefinition> columns;
    private final List<ForeignKeySchema> foreignKeys;

    public ParsedQueryCreate(String tableName, List<ColumnDefinition> columns, List<ForeignKeySchema> foreignKeys) {
        super(tableName, DDLType.CREATE_TABLE);
        this.columns = columns != null ? columns : new ArrayList<>();
        this.foreignKeys = foreignKeys != null ? foreignKeys : new ArrayList<>();
    }

    public List<ColumnDefinition> getColumns() {
        return columns;
    }

    public List<ForeignKeySchema> getForeignKeys() {
        return foreignKeys;
    }
}