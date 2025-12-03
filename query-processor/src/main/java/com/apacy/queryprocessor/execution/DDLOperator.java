package com.apacy.queryprocessor.execution;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.apacy.common.dto.Column;
import com.apacy.common.dto.IndexSchema;
import com.apacy.common.dto.Row;
import com.apacy.common.dto.Schema;
import com.apacy.common.dto.ddl.ColumnDefinition;
import com.apacy.common.dto.ddl.ParsedQueryCreate;
import com.apacy.common.dto.ddl.ParsedQueryDDL;
import com.apacy.common.dto.ddl.ParsedQueryDrop;
import com.apacy.common.dto.plan.DDLNode;
import com.apacy.common.enums.IndexType;
import com.apacy.common.interfaces.IStorageManager;

public class DDLOperator implements Operator {
    private final DDLNode node;
    private final IStorageManager sm;
    private boolean executed = false;

    public DDLOperator(DDLNode node, IStorageManager sm) {
        this.node = node;
        this.sm = sm;
    }

    @Override
    public void open() {
        ParsedQueryDDL ddlQuery = node.ddlQuery(); 

        try {
            // CREATE TABLE
            if (ddlQuery instanceof ParsedQueryCreate createCmd) {
                Schema schema = translateToSchema(createCmd);
                sm.createTable(schema);
            }
            // DROP TABLE
            else if (ddlQuery instanceof ParsedQueryDrop dropCmd) {
                // sm.dropTable(dropCmd.getTableName(), dropCmd.isCascading());
            }
        } catch (IOException e) {
            throw new RuntimeException("Storage IO Error executing DDL: " + e.getMessage(), e);
        }
        executed = true;
    }

    @Override
    public Row next() {
        return null; // DDL returns no rows
    }

    @Override
    public void close() {
    }

    private Schema translateToSchema(ParsedQueryCreate query) {
        String tableName = query.getTableName();
        String dataFileName = tableName + ".dat";

        List<Column> smColumns = new ArrayList<>();
        List<IndexSchema> smIndexes = new ArrayList<>();

        for (ColumnDefinition colDef : query.getColumns()) {
            Column col = new Column(colDef.getName(), colDef.getType(), colDef.getLength());
            smColumns.add(col);

            if (colDef.isPrimaryKey()) {
                String indexName = "pk_" + tableName + "_" + colDef.getName();
                String indexFile = tableName + "_" + colDef.getName() + ".idx";
                
                IndexSchema pkIndex = new IndexSchema(indexName, colDef.getName(), IndexType.Hash, indexFile);
                smIndexes.add(pkIndex);
            }
        }

        return new Schema(
            tableName, 
            dataFileName, 
            smColumns, 
            smIndexes, 
            query.getForeignKeys() 
        );
    }
}
