package com.apacy.queryprocessor.mocks;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import com.apacy.common.dto.ForeignKeySchema;
import com.apacy.common.dto.ParsedQuery;
import com.apacy.common.dto.ddl.ColumnDefinition;
import com.apacy.common.dto.ddl.ParsedQueryCreate;
import com.apacy.common.dto.ddl.ParsedQueryDDL;
import com.apacy.common.dto.ddl.ParsedQueryDrop;
import com.apacy.common.enums.DataType;
import com.apacy.queryoptimizer.QueryTokenizer;
import com.apacy.queryoptimizer.parser.AbstractParser;
import com.apacy.queryoptimizer.parser.Token;
import com.apacy.queryoptimizer.parser.TokenType;

/**
 * Mock Parser for DDL Statements.
 * Returns ParsedQueryDDL objects (Create, Drop).
 */
public class MockDDLParser extends AbstractParser {

    public MockDDLParser(String sql) {
        super(new QueryTokenizer(sql).tokenize());
    }

    // Abstract methods (gak dipake)
    @Override
    public ParsedQuery parse() throws ParseException { return null; }
    @Override
    public boolean validate() { return true; }

    /**
     * Main Entry Point
     */
    public ParsedQueryDDL parseDDL() {
        Token t = peek();

        if (t.getType() == TokenType.CREATE) {
            return parseCreateTable();
        } 
        else if (t.getType() == TokenType.DROP) {
            return parseDropTable();
        } 
        
        throw new RuntimeException("Unknown or unsupported DDL Command: " + t.getValue());
    }

    public boolean isDDL() {
        if (tokens.isEmpty()) return false;
        Token t = peek();
        return t.getType() == TokenType.CREATE || 
               t.getType() == TokenType.DROP;
    }

    // Create Table
    private ParsedQueryCreate parseCreateTable() {
        consume(TokenType.CREATE);
        consume(TokenType.TABLE);

        String tableName = consume(TokenType.IDENTIFIER).getValue();
        consume(TokenType.LPARENTHESIS);

        List<ColumnDefinition> columns = new ArrayList<>();
        List<ForeignKeySchema> foreignKeys = new ArrayList<>();

        boolean moreDefinitions = true;
        while (moreDefinitions) {            
            if (isIdentifier("FOREIGN")) {
                foreignKeys.add(parseForeignKey(tableName));
            } else {
                columns.add(parseColumnDefinition());
            }

            moreDefinitions = match(TokenType.COMMA);
        }

        consume(TokenType.RPARENTHESIS);

        return new ParsedQueryCreate(tableName, columns, foreignKeys);
    }

    private ColumnDefinition parseColumnDefinition() {
        String colName = consume(TokenType.IDENTIFIER).getValue();

        Token typeToken = consume(TokenType.IDENTIFIER);
        String typeStr = typeToken.getValue().toUpperCase();
        
        DataType type = DataType.VARCHAR;
        int length = 0;

        switch (typeStr) {
            case "INT":
            case "INTEGER":
                type = DataType.INTEGER;
                break;
            case "FLOAT":
            case "DOUBLE":
                type = DataType.FLOAT;
                break;
            case "CHAR":
                type = DataType.CHAR;
                length = 1;
                break;
            case "VARCHAR":
            case "STRING":
                type = DataType.VARCHAR;
                if (peek().getType() == TokenType.LPARENTHESIS) {
                    consume(TokenType.LPARENTHESIS);
                    length = Integer.parseInt(consume(TokenType.NUMBER_LITERAL).getValue());
                    consume(TokenType.RPARENTHESIS);
                } else {
                    length = 255; // Default
                }
                break;
            default:
                type = DataType.VARCHAR;
                length = 255;
        }

        boolean isPrimary = false;
        if (isIdentifier("PRIMARY")) {
            consumeKeyword("PRIMARY");
            consumeKeyword("KEY");
            isPrimary = true;
        }

        return new ColumnDefinition(colName, type, length, isPrimary);
    }

    private ForeignKeySchema parseForeignKey(String currentTable) {
        consumeKeyword("FOREIGN");
        consumeKeyword("KEY");
        
        consume(TokenType.LPARENTHESIS);
        String colName = consume(TokenType.IDENTIFIER).getValue();
        consume(TokenType.RPARENTHESIS);

        consumeKeyword("REFERENCES");
        String refTable = consume(TokenType.IDENTIFIER).getValue();
        
        consume(TokenType.LPARENTHESIS);
        String refCol = consume(TokenType.IDENTIFIER).getValue();
        consume(TokenType.RPARENTHESIS);

        boolean isCascading = false;
        // Check ON DELETE CASCADE
        if (isIdentifier("ON")) {
            consumeKeyword("ON");
            consumeKeyword("DELETE");
            if (isIdentifier("CASCADE")) {
                consumeKeyword("CASCADE");
                isCascading = true;
            } else if (isIdentifier("RESTRICT")) {
                consumeKeyword("RESTRICT");
                isCascading = false;
            }
        }

        String constraintName = "fk_" + currentTable + "_" + colName;
        return new ForeignKeySchema(constraintName, colName, refTable, refCol, isCascading);
    }

    // Drop Table
    private ParsedQueryDrop parseDropTable() {
        consume(TokenType.DROP);
        consume(TokenType.TABLE);
        
        String tableName = consume(TokenType.IDENTIFIER).getValue();
        
        boolean isCascading = false;
        if (isIdentifier("CASCADE")) {
            consumeKeyword("CASCADE");
            isCascading = true;
        } else if (isIdentifier("RESTRICT")) {
            consumeKeyword("RESTRICT");
        }

        return new ParsedQueryDrop(tableName, isCascading);
    }

    /**
     * Helper to check if current token is IDENTIFIER with specific value (case-insensitive).
     * DDL Keyword ga ada di TokenType.
     */
    private boolean isIdentifier(String val) {
        Token t = peek();
        return t.getType() == TokenType.IDENTIFIER && t.getValue().equalsIgnoreCase(val);
    }

    /**
     * Helper to consumes an identifier that acts as a Keyword.
     */
    private void consumeKeyword(String keyword) {
        Token t = peek();
        if (t.getType() == TokenType.IDENTIFIER && t.getValue().equalsIgnoreCase(keyword)) {
            position++;
        } else {
        }
    }
}