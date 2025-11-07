package com.apacy.queryoptimizer.parser;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.text.ParseException;
import java.util.List;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.apacy.common.dto.ParsedQuery;

@Disabled("Disabled karena InsertParser belum diimplementasi. Hilangkan baris ini untuk mengenable")
class InsertParserTest {

    // --- Test Validate ---

    @Test
    void validate_ValidInsertQuery_ShouldReturnTrue() {
        /**
         * INSERT INTO logs VALUES ('INFO', 'Started');
         */
        List<Token> tokens = List.of(
            new Token(TokenType.INSERT, "INSERT"), new Token(TokenType.INTO, "INTO"), new Token(TokenType.IDENTIFIER, "logs"),
            new Token(TokenType.VALUES, "VALUES"), new Token(TokenType.LPARENTHESIS, "("), new Token(TokenType.STRING_LITERAL, "'INFO'"), new Token(TokenType.COMMA, ","), new Token(TokenType.STRING_LITERAL, "'Started'"), new Token(TokenType.RPARENTHESIS, ")"),
            new Token(TokenType.SEMICOLON, ";"), new Token(TokenType.EOF, null)
        );

        assertTrue(new InsertParser(tokens).validate(), "Valid INSERT query should pass validation.");
    }

    @Test
    void validate_InvalidInsertSyntax_ShouldReturnFalse() {
        /**
         * query: INSERT logs VALUES
         */
        List<Token> tokens = List.of(
            new Token(TokenType.INSERT, "INSERT"), new Token(TokenType.IDENTIFIER, "logs"),
            new Token(TokenType.VALUES, "VALUES"), new Token(TokenType.EOF, null)
        );

        assertFalse(new InsertParser(tokens).validate(), "Invalid INSERT query should not pass validation.");
    }

    // --- Test Parse ---

    @Test
    void parse_BasicInsertQuery_ShouldMatchExpectedParsedQuery() throws ParseException {
        /**
         * query: INSERT INTO products VALUES  ('Laptop', 1500);
         */
        List<Token> tokens = List.of(
            new Token(TokenType.INSERT, "INSERT"), new Token(TokenType.INTO, "INTO"), new Token(TokenType.IDENTIFIER, "products"),
            new Token(TokenType.LPARENTHESIS, "("), new Token(TokenType.IDENTIFIER, "name"), new Token(TokenType.COMMA, ","), new Token(TokenType.IDENTIFIER, "price"), new Token(TokenType.RPARENTHESIS, ")"),
            new Token(TokenType.VALUES, "VALUES"), new Token(TokenType.LPARENTHESIS, "("), new Token(TokenType.STRING_LITERAL, "'Laptop'"), new Token(TokenType.COMMA, ","), new Token(TokenType.NUMBER_LITERAL, "1500"), new Token(TokenType.RPARENTHESIS, ")"),
            new Token(TokenType.SEMICOLON, ";"), new Token(TokenType.EOF, null)
        );

        ParsedQuery actual = new InsertParser(tokens).parse();
        assertEquals("INSERT", actual.queryType());
        assertEquals(List.of("products"), actual.targetTables());
        assertEquals(List.of("name", "price"), actual.targetColumns());

        assertThrows(UnsupportedOperationException.class, () -> new InsertParser(tokens).parse());
    }
}