package com.apacy.queryoptimizer.parser;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import com.apacy.common.dto.ForeignKeySchema;
import com.apacy.common.dto.ParsedQuery;
import com.apacy.common.dto.ddl.ColumnDefinition;
import com.apacy.common.dto.ddl.ParsedQueryCreate;
import com.apacy.common.dto.ddl.ParsedQueryCreateIndex;
import com.apacy.common.dto.ddl.ParsedQueryDDL;
import com.apacy.common.dto.ddl.ParsedQueryDrop;
import com.apacy.common.dto.ddl.ParsedQueryDropIndex;
import com.apacy.common.dto.plan.DDLNode;
import com.apacy.common.dto.plan.PlanNode;
import com.apacy.common.enums.DataType;

/**
 * Parser for DDL Statements.
 * Returns ParsedQueryDDL objects (Create, Drop).
 */
public class DDLParser extends AbstractParser {

    public DDLParser(List<Token> tokens) {
        super(tokens);
    }

    @Override
    public ParsedQuery parse() throws ParseException {
        Token t = peek();

        if (t.getType() == TokenType.CREATE) {
            consume(TokenType.CREATE);
            t = peek();
            if (t.getType() == TokenType.TABLE) {
                return parseCreateTable();
            } else if (t.getType() == TokenType.INDEX) {
                return ParseCreateIndex();
            }
        }
        else if (t.getType() == TokenType.DROP) {
            consume(TokenType.DROP);
            t = peek();
            if (t.getType() == TokenType.TABLE) {
                return parseDropTable();
            } else if (t.getType() == TokenType.INDEX) {
                return ParseDropIndex();
            }
        }

        throw new RuntimeException("Unknown or unsupported DDL Command: " + t.getValue());
    }

    @Override
    public boolean validate() { return true; }

    // Create Table
    private ParsedQuery parseCreateTable() {
        // consume(TokenType.CREATE);
        consume(TokenType.TABLE);

        String tableName = consume(TokenType.IDENTIFIER).getValue();
        consume(TokenType.LPARENTHESIS);

        List<ColumnDefinition> columns = new ArrayList<>();
        List<ForeignKeySchema> foreignKeys = new ArrayList<>();

        boolean moreDefinitions = true;
        while (moreDefinitions) {
            if (match(TokenType.FOREIGN)) {
                foreignKeys.add(parseForeignKey(tableName));
            } else {
                columns.add(parseColumnDefinition());
            }

            moreDefinitions = match(TokenType.COMMA);
        }

        consume(TokenType.RPARENTHESIS);

        ParsedQueryDDL ddl = new ParsedQueryCreate(tableName, columns, foreignKeys);
        PlanNode planRoot = new DDLNode(ddl);

        return new ParsedQuery(
            "CREATE",
            planRoot,
            null,
            null,
            null,
            null,
            null,
            null,
            false,
            false
        );
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
                // type = DataType.VARCHAR;
                // length = 255;
                throw new RuntimeException("Invalid attribute type");
        }

        boolean isPrimary = false;
        if (match(TokenType.PRIMARY)) {
            consume(TokenType.KEY);
            isPrimary = true;
        }

        return new ColumnDefinition(colName, type, length, isPrimary);
    }

    private ForeignKeySchema parseForeignKey(String currentTable) {
        // consume(TokenType.FOREIGN);
        consume(TokenType.KEY);

        consume(TokenType.LPARENTHESIS);
        String colName = consume(TokenType.IDENTIFIER).getValue();
        consume(TokenType.RPARENTHESIS);

        consume(TokenType.REFERENCES);
        String refTable = consume(TokenType.IDENTIFIER).getValue();

        consume(TokenType.LPARENTHESIS);
        String refCol = consume(TokenType.IDENTIFIER).getValue();
        consume(TokenType.RPARENTHESIS);

        boolean isCascading = false;
        // Check ON DELETE CASCADE
        if (match(TokenType.ON)) {
            consume(TokenType.DELETE);
            if (match(TokenType.CASCADE)) {
                isCascading = true;
            } else if (match(TokenType.RESTRICT)) {
                isCascading = false;
            } else {
                throw new RuntimeException("ON DELETE rule must be either CASCADE or RESTRICT");
            }


        }

        String constraintName = "fk_" + currentTable + "_" + colName;
        return new ForeignKeySchema(constraintName, colName, refTable, refCol, isCascading);
    }

    // Drop Table
    private ParsedQuery parseDropTable() {
        // consume(TokenType.DROP);
        consume(TokenType.TABLE);

        String tableName = consume(TokenType.IDENTIFIER).getValue();

        boolean isCascading = false;
        if (match(TokenType.CASCADE)) {
            isCascading = true;
        } else if (match(TokenType.RESTRICT)) {}

        ParsedQueryDDL ddl = new ParsedQueryDrop(tableName, isCascading);
        PlanNode planRoot = new DDLNode(ddl);

        return new ParsedQuery(
            "DROP",
            planRoot,
            null,
            null,
            null,
            null,
            null,
            null,
            false,
            false
        );
    }

    private ParsedQuery ParseCreateIndex() {
        // consume(TokenType.CREATE);
        consume(TokenType.INDEX);

        String indexName = consume(TokenType.IDENTIFIER).getValue();

        consume(TokenType.ON);

        String tableName = consume(TokenType.IDENTIFIER).getValue();

        consume(TokenType.USING);

        Token indexType = peek();
        if (match(TokenType.BTREE) || match(TokenType.HASH)) {}
        else {
            throw new RuntimeException("Expected BTREE or HASH index type.");
        }

        consume(TokenType.LPARENTHESIS);
        String columnName = consume(TokenType.IDENTIFIER).getValue();
        consume(TokenType.RPARENTHESIS);

        consume(TokenType.SEMICOLON);
        consume(TokenType.EOF);

        ParsedQueryDDL ddl = new ParsedQueryCreateIndex(tableName, indexName, columnName, indexType.getValue());
        PlanNode planRoot = new DDLNode(ddl);

        return new ParsedQuery(
            "CREATE INDEX",
            planRoot,
            null,
            null,
            null,
            null,
            null,
            null,
            false,
            false
        );
    }

    private ParsedQuery ParseDropIndex() {
        // consume(TokenType.DROP);
        consume(TokenType.INDEX);

        String indexName = consume(TokenType.IDENTIFIER).getValue();

        consume(TokenType.SEMICOLON);
        consume(TokenType.EOF);

        ParsedQueryDDL ddl = new ParsedQueryDropIndex(indexName);
        PlanNode planRoot = new DDLNode(ddl);

        return new ParsedQuery(
            "DROP INDEX",
            planRoot,
            null,
            null,
            null,
            null,
            null,
            null,
            false,
            false
        );
    }

}