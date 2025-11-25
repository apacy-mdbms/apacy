package com.apacy.common.dto.plan;

import java.util.List;

import com.apacy.common.dto.ddl.ParsedQueryDDL;

/**
 * Node pembungkus untuk perintah DDL (CREATE, DROP, ALTER).
 * Tidak punya child. Tidak akan dioptimasi.
 */
public record DDLNode(
    ParsedQueryDDL ddlQuery
) implements PlanNode {

    @Override 
    public List<PlanNode> getChildren() { 
        return List.of(); 
    }

    @Override 
    public String toString() { 
        return "DDL: " + ddlQuery.getType() + " " + ddlQuery.getTableName(); 
    }
}