package com.apacy.queryoptimizer.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.text.ParseException;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.apacy.common.dto.ParsedQuery;
import com.apacy.common.dto.ast.expression.ColumnFactor;
import com.apacy.common.dto.ast.expression.ExpressionNode;
import com.apacy.common.dto.ast.expression.LiteralFactor;
import com.apacy.common.dto.ast.expression.TermNode;
import com.apacy.common.dto.ast.where.ComparisonConditionNode;
import com.apacy.common.dto.ast.where.WhereConditionNode;

class DeleteParserTest {

    // Helper untuk WHERE sederhana: status = 'PENDING'
    // @SuppressWarnings("unused")
    private WhereConditionNode createSimpleWhereNode() {
        ExpressionNode left = new ExpressionNode(
            new TermNode(new ColumnFactor("status"), List.of()),
            List.of()
        );
        ExpressionNode right = new ExpressionNode(
            new TermNode(new LiteralFactor("'PENDING'"), List.of()),
            List.of()
        );
        return new ComparisonConditionNode(left, "=", right);
    }

    // --- Test Validate ---

    @Test
    void validate_ValidDeleteQuery_ShouldReturnTrue() {
        // DELETE FROM temp_logs WHERE status = 'PENDING';
        List<Token> tokens = List.of(
            new Token(TokenType.DELETE, "DELETE"), new Token(TokenType.FROM, "FROM"), new Token(TokenType.IDENTIFIER, "temp_logs"),
            new Token(TokenType.WHERE, "WHERE"), new Token(TokenType.IDENTIFIER, "status"), new Token(TokenType.OPERATOR, "="), new Token(TokenType.STRING_LITERAL, "'PENDING'"),
            new Token(TokenType.SEMICOLON, ";"), new Token(TokenType.EOF, null)
        );

        assertTrue(new DeleteParser(tokens).validate(), "Valid DELETE query should pass validation.");
    }

    @Test
    void validate_InvalidDeleteSyntax_ShouldReturnFalse() {
        // DELETE temp_logs (Kurang FROM)
        List<Token> tokens = List.of(
            new Token(TokenType.DELETE, "DELETE"), new Token(TokenType.IDENTIFIER, "temp_logs"),
            new Token(TokenType.EOF, null)
        );

        assertFalse(new DeleteParser(tokens).validate(),
            "Invalid syntax (missing FROM) should throw a parsing exception.");
    }

    // --- Test Parse ---

    @Test
    void parse_DeleteQueryWithWhere_ShouldMatchExpectedParsedQuery() throws ParseException {

        // DELETE FROM cache WHERE last_access < 1000;

        List<Token> tokens = List.of(
            new Token(TokenType.DELETE, "DELETE"), new Token(TokenType.FROM, "FROM"), new Token(TokenType.IDENTIFIER, "cache"),
            new Token(TokenType.WHERE, "WHERE"), new Token(TokenType.IDENTIFIER, "last_access"), new Token(TokenType.OPERATOR, "<"), new Token(TokenType.NUMBER_LITERAL, "1000"),
            new Token(TokenType.SEMICOLON, ";"), new Token(TokenType.EOF, null)
        );

        ParsedQuery actual = new DeleteParser(tokens).parse();
        assertEquals("DELETE", actual.queryType());
        assertEquals(List.of("cache"), actual.targetTables());
        assertNotNull(actual.whereClause());
        //... (Cek WHERE clause)
    }
}