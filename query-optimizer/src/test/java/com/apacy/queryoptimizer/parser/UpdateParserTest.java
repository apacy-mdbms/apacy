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

@Disabled("Disabled karena UpdateParser belum diimplementasi. Hilangkan baris ini untuk mengenable")
class UpdateParserTest {

    // Helper untuk WHERE sederhana: id = 5
    private WhereConditionNode createSimpleWhereNode() {
        return new ComparisonConditionNode("id", "=", 5);
    }

    // --- Test Validate ---

    @Test
    void validate_ValidUpdateQuery_ShouldReturnTrue() {
        /**
         * query:  UPDATE users SET active = 1 WHERE id = 5;
         */
        List<Token> tokens = List.of(
            new Token(TokenType.UPDATE, "UPDATE"), new Token(TokenType.IDENTIFIER, "users"),
            new Token(TokenType.SET, "SET"), new Token(TokenType.IDENTIFIER, "active"), new Token(TokenType.OPERATOR, "="), new Token(TokenType.NUMBER_LITERAL, "1"),
            new Token(TokenType.WHERE, "WHERE"), new Token(TokenType.IDENTIFIER, "id"), new Token(TokenType.OPERATOR, "="), new Token(TokenType.NUMBER_LITERAL, "5"),
            new Token(TokenType.SEMICOLON, ";"), new Token(TokenType.EOF, null)
        );

        assertTrue(new UpdateParser(tokens).validate(), "Valid UPDATE query should pass validation.");
    }

    @Test
    void validate_InvalidUpdateSyntax_ShouldReturnFalse() {
        /**
         * query:  UPDATE users active = 1 WHERE id = 5;
         *
         * invalid:
         * - no SET keyword
         */
        List<Token> tokens = List.of(
            new Token(TokenType.UPDATE, "UPDATE"), new Token(TokenType.IDENTIFIER, "users"),
            new Token(TokenType.IDENTIFIER, "active"), new Token(TokenType.OPERATOR, "="), new Token(TokenType.NUMBER_LITERAL, "1"),
            new Token(TokenType.EOF, null)
        );

        assertFalse(new UpdateParser(tokens).validate(), "Invalid UPDATE query should not pass validation.");
    }

    // --- Test Parse ---

    @Test
    void parse_UpdateQueryWithComplexSet_ShouldMatchExpectedParsedQuery() throws ParseException {
        // UPDATE products SET price = price * 1.1, stock = stock - 1 WHERE id > 10;
        List<Token> tokens = List.of(
            new Token(TokenType.UPDATE, "UPDATE"), new Token(TokenType.IDENTIFIER, "products"),
            new Token(TokenType.SET, "SET"),
                new Token(TokenType.IDENTIFIER, "price"), new Token(TokenType.OPERATOR, "="), new Token(TokenType.IDENTIFIER, "price"), new Token(TokenType.STAR, "*"), new Token(TokenType.NUMBER_LITERAL, "1.1"), new Token(TokenType.COMMA, ","),
                new Token(TokenType.IDENTIFIER, "stock"), new Token(TokenType.OPERATOR, "="), new Token(TokenType.IDENTIFIER, "stock"), new Token(TokenType.OPERATOR, "-"), new Token(TokenType.NUMBER_LITERAL, "1"),
            new Token(TokenType.WHERE, "WHERE"), new Token(TokenType.IDENTIFIER, "id"), new Token(TokenType.OPERATOR, ">"), new Token(TokenType.NUMBER_LITERAL, "10"),
            new Token(TokenType.SEMICOLON, ";"), new Token(TokenType.EOF, null)
        );

        ParsedQuery actual = new UpdateParser(tokens).parse();
        assertEquals("UPDATE", actual.queryType());
        assertEquals(List.of("products"), actual.targetTables());
        assertNotNull(actual.whereClause());
        assertTrue(actual.whereClause() instanceof ComparisonConditionNode, "WHERE id > 10 should be ComparisonConditionNode");
        // ... (Cek WHERE clause)
    }
}