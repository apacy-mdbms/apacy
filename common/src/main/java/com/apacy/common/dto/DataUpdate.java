package com.apacy.common.dto;

import com.apacy.common.dto.ast.where.WhereConditionNode;


/**
 * DTO untuk operasi UPDATE
 * @param tableName 
 * @param updatedData
 * @param filterCondition
 */
public record DataUpdate(
    String tableName, 
    Row updatedData, 
    WhereConditionNode filterCondition) {}