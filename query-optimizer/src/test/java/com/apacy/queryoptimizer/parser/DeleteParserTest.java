package com.apacy.queryoptimizer.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.text.ParseException;
import java.util.List;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.apacy.common.dto.ParsedQuery;
import com.apacy.queryoptimizer.ast.where.ComparisonConditionNode;
import com.apacy.queryoptimizer.ast.where.WhereConditionNode;

@Disabled("Disabled karena DeleteParser belum diimplementasi. Hilangkan baris ini untuk mengenable")
class DeleteParserTest {

    // Helper untuk WHERE sederhana: status = 'PENDING'
    private WhereConditionNode createSimpleWhereNode() {
        return new ComparisonConditionNode("status", "=", "'PENDING'");
    }

    // --- Test Validate ---

    @Test
    void validate_ValidDeleteQuery_ShouldReturnTrue() {
        // DELETE FROM temp_logs WHERE status = 'PENDING';
        List<Token> tokens = List.of(
            new Token(TokenType.DELETE, "DELETE"), new Token(TokenType.FROM, "FROM"), new Token(TokenType.IDENTIFIER, "temp_logs"),
            new Token(TokenType.WHERE, "WHERE"), new Token(TokenType.IDENTIFIER, "status"), new Token(TokenType.OPERATOR, "="), new Token(TokenType.STRING_LITERAL, "'PENDING'"),
            new Token(TokenType.SEMICOLON, ";"), new Token(TokenType.EOF, "")
        );

        assertTrue(new DeleteParser(tokens).validate(), "Valid DELETE query should pass validation.");
    }

    @Test
    void validate_InvalidDeleteSyntax_ShouldReturnFalse() {
        // DELETE temp_logs (Kurang FROM)
        List<Token> tokens = List.of(
            new Token(TokenType.DELETE, "DELETE"), new Token(TokenType.IDENTIFIER, "temp_logs"),
            new Token(TokenType.EOF, "")
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
            new Token(TokenType.SEMICOLON, ";"), new Token(TokenType.EOF, "")
        );

        ParsedQuery actual = new DeleteParser(tokens).parse();
        assertEquals("DELETE", actual.queryType());
        assertEquals(List.of("cache"), actual.targetTables());
        assertNotNull(actual.whereClause());
        //... (Cek WHERE clause)
    }
}