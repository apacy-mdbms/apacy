package com.apacy.queryoptimizer.parser;

import java.text.ParseException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

import com.apacy.common.dto.ParsedQuery;
import com.apacy.queryoptimizer.ast.expression.ColumnFactor;
import com.apacy.queryoptimizer.ast.expression.ExpressionNode;
import com.apacy.queryoptimizer.ast.expression.LiteralFactor;
import com.apacy.queryoptimizer.ast.expression.TermNode;
import com.apacy.queryoptimizer.ast.join.JoinConditionNode;
import com.apacy.queryoptimizer.ast.join.JoinOperand;
import com.apacy.queryoptimizer.ast.join.TableNode;
import com.apacy.queryoptimizer.ast.where.BinaryConditionNode;
import com.apacy.queryoptimizer.ast.where.ComparisonConditionNode;
import com.apacy.queryoptimizer.ast.where.WhereConditionNode;

class SelectParserTest {

    // Helper untuk membuat AST WHERE yang kompleks: col1 = 10 AND (col2 > 5 OR col3 < 1)
    private WhereConditionNode createExpectedWhereNode() {
        ComparisonConditionNode rightBinLeft = new ComparisonConditionNode(new ExpressionNode(new TermNode(new ColumnFactor("col2"), List.of()), List.of()), ">", new ExpressionNode(new TermNode(new LiteralFactor(5), List.of()), List.of()));
        ComparisonConditionNode rightBinRight = new ComparisonConditionNode(new ExpressionNode(new TermNode(new ColumnFactor("col3"), List.of()), List.of()), "<", new ExpressionNode(new TermNode(new LiteralFactor(1), List.of()), List.of()));
        BinaryConditionNode rightBinary = new BinaryConditionNode(rightBinLeft, "OR", rightBinRight);
        ComparisonConditionNode left = new ComparisonConditionNode(new ExpressionNode(new TermNode(new ColumnFactor("col1"), List.of()), List.of()), "=", new ExpressionNode(new TermNode(new LiteralFactor(10), List.of()), List.of()));
        return new BinaryConditionNode(left, "AND", rightBinary);
    }

    // Helper untuk membuat AST JOIN yang kompleks: table1 INNER JOIN table2 ON table1.id = table2.fk
    private JoinOperand createExpectedJoinNode() {
        TableNode leftTable = new TableNode("table1");
        TableNode rightTable = new TableNode("table2");
        ComparisonConditionNode condition = new ComparisonConditionNode(new ExpressionNode(new TermNode(new ColumnFactor("table1.id"), null), null), "=", new ExpressionNode(new TermNode(new ColumnFactor("table2.fk"), null), null));
        return new JoinConditionNode("INNER", leftTable, rightTable, condition);
    }


    // --- Test Validate ---

    @Test
    void validate_ValidSelectQuery_ShouldReturnTrue() {

        /**
         * query: SELECT * FROM users;
         */

        List<Token> tokens = List.of(
            new Token(TokenType.SELECT, "SELECT"),
            new Token(TokenType.STAR, "*"),
            new Token(TokenType.FROM, "FROM"),
            new Token(TokenType.IDENTIFIER, "users"),
            new Token(TokenType.SEMICOLON, ";"),
            new Token(TokenType.EOF, null)
        );

        AbstractParser parser = new SelectParser(tokens);
        assertTrue(parser.validate(), "Valid SELECT query should pass validation.");

    }

    @Test
    void validate_InvalidSelectSyntax_ShouldReturnFalse() {

        /**
         * query: SELECT * FROM WHERE
         *
         * tidak valid:
         * - no table
         * - invalid where condition
         * - no semicolon
         */

        List<Token> tokens = List.of(
            new Token(TokenType.SELECT, "SELECT"),
            new Token(TokenType.STAR, "*"),
            new Token(TokenType.FROM, "FROM"),
            new Token(TokenType.WHERE, "WHERE"),
            new Token(TokenType.EOF, null)
        );

        assertFalse(new SelectParser(tokens).validate());
    }

    // --- Test Parse ---

    @Test
    void parse_ComplexSelectQuery_ShouldMatchExpectedParsedQuery() throws ParseException {
        // SELECT col1, col2 FROM table1 INNER JOIN table2 ON table1.id = table2.fk WHERE col1 = 10 AND (col2 > 5 OR col3 < 1) ORDER BY col1 LIMIT 10;
        /**
         * query:
         *
         * SELECT col1, col2
         * FROM table1
         *      JOIN table2 ON table1.id = tabl2.fk
         * WHERE
         *      col1 = 10
         *      AND
         *      (col2 > 5 OR col3 < 1)
         * ORDER BY col1
         * LIMIT 10;
         *
         */
        List<Token> tokens = List.of(
            new Token(TokenType.SELECT, "SELECT"), new Token(TokenType.IDENTIFIER, "col1"), new Token(TokenType.COMMA, ","), new Token(TokenType.IDENTIFIER, "col2"),
            new Token(TokenType.FROM, "FROM"), new Token(TokenType.IDENTIFIER, "table1"),
            new Token(TokenType.JOIN, "JOIN"), new Token(TokenType.IDENTIFIER, "table2"), new Token(TokenType.ON, "ON"), new Token(TokenType.IDENTIFIER, "table1.id"), new Token(TokenType.OPERATOR, "="), new Token(TokenType.IDENTIFIER, "table2.fk"),
            new Token(TokenType.WHERE, "WHERE"),
                new Token(TokenType.IDENTIFIER, "col1"), new Token(TokenType.OPERATOR, "="), new Token(TokenType.NUMBER_LITERAL, "10"),
                new Token(TokenType.AND, "AND"), new Token(TokenType.LPARENTHESIS, "("),
                    new Token(TokenType.IDENTIFIER, "col2"), new Token(TokenType.OPERATOR, ">"), new Token(TokenType.NUMBER_LITERAL, "5"),
                    new Token(TokenType.OR, "OR"),
                    new Token(TokenType.IDENTIFIER, "col3"), new Token(TokenType.OPERATOR, "<"), new Token(TokenType.NUMBER_LITERAL, "1"),
                new Token(TokenType.RPARENTHESIS, ")"),
            new Token(TokenType.ORDER, "ORDER"), new Token(TokenType.BY, "BY"), new Token(TokenType.IDENTIFIER, "col1"),
            new Token(TokenType.LIMIT, "LIMIT"), new Token(TokenType.NUMBER_LITERAL, "10"),
            new Token(TokenType.SEMICOLON, ";"), new Token(TokenType.EOF, null)
        );

        AbstractParser parser = new SelectParser(tokens);

        ParsedQuery actual = parser.parse();

        // Tipe Kueri
        assertEquals("SELECT", actual.queryType(), "Query Type should be SELECT.");

        // Kolom Target
        assertEquals(List.of("col1", "col2"), actual.targetColumns(), "Target columns should match.");

        // Tabel Target (Harus berisi tabel dasar yang terlibat)
        // Asumsi: targetTables berisi semua tabel yang terlibat
        assertEquals(List.of("table1", "table2"), actual.targetTables(), "Target tables should match.");

        // Join Conditions (AST)
        assertEquals(createExpectedJoinNode(), actual.joinConditions(), "Join AST should match expected structure.");

        // Where Clause (AST)
        assertEquals(createExpectedWhereNode(), actual.whereClause(), "Where clause AST should match expected structure.");

        // Order By
        assertEquals("col1", actual.orderByColumn(), "ORDER BY column should be 'col1'.");
        assertEquals(false, actual.isDescending(), "isDescending should be false (default ASC).");

        // Status default
        assertEquals(false, actual.isOptimized(), "isOptimized should be false by default.");
    }

    @Test
    void parse_SelectWithLimitAndOffset_ShouldMatch() throws ParseException {
        // Query: SELECT * FROM users LIMIT 10 OFFSET 5;
        List<Token> tokens = List.of(
            new Token(TokenType.SELECT, "SELECT"),
            new Token(TokenType.STAR, "*"),
            new Token(TokenType.FROM, "FROM"),
            new Token(TokenType.IDENTIFIER, "users"),
            new Token(TokenType.LIMIT, "LIMIT"),
            new Token(TokenType.NUMBER_LITERAL, "10"),
            new Token(TokenType.OFFSET, "OFFSET"),
            new Token(TokenType.NUMBER_LITERAL, "5"),
            new Token(TokenType.SEMICOLON, ";"),
            new Token(TokenType.EOF, null)
        );

        AbstractParser parser = new SelectParser(tokens);
        ParsedQuery actual = parser.parse();

        // Verifikasi Limit & Offset
        assertEquals(10, actual.limit(), "Limit harus 10");
        assertEquals(5, actual.offset(), "Offset harus 5");
    }
    
    @Test
    void validate_SelectWithLimitOffset_ShouldReturnTrue() {
        // Query: SELECT * FROM users LIMIT 10 OFFSET 5;
        List<Token> tokens = List.of(
            new Token(TokenType.SELECT, "SELECT"),
            new Token(TokenType.STAR, "*"),
            new Token(TokenType.FROM, "FROM"),
            new Token(TokenType.IDENTIFIER, "users"),
            new Token(TokenType.LIMIT, "LIMIT"),
            new Token(TokenType.NUMBER_LITERAL, "10"),
            new Token(TokenType.OFFSET, "OFFSET"),
            new Token(TokenType.NUMBER_LITERAL, "5"),
            new Token(TokenType.SEMICOLON, ";"),
            new Token(TokenType.EOF, null)
        );

        AbstractParser parser = new SelectParser(tokens);
        assertTrue(parser.validate(), "Validasi SELECT dengan LIMIT/OFFSET harus true");
    }
}