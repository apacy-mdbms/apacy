package com.apacy.common.dto;

/**
 * DTO untuk operasi UPDATE
 * @param tableName 
 * @param updatedData
 * @param filterCondition
 */
public record DataUpdate(String tableName, Row updatedData, Object filterCondition) {}