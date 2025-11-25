package com.apacy.queryoptimizer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.apacy.queryoptimizer.parser.Token;
import com.apacy.queryoptimizer.parser.TokenType;

class QueryTokenizerTest {

    /**
     * Helper method untuk membuat dan menjalankan tokenizer.
     * @param input Kueri SQL.
     * @return List token yang dihasilkan.
     */
    private List<Token> tokenize(String input) {
        QueryTokenizer tokenizer = new QueryTokenizer(input);
        return tokenizer.tokenize();
    }

    /**
     * Helper method untuk compare tipe dan lexeme token
     */
    private void assertToken(TokenType expectedType, String expectedValue, Token actualToken) {
        assertEquals(expectedType, actualToken.getType(),
            "Tipe token salah. Diharapkan: " + expectedType + ", Aktual: " + actualToken.getType() +
            " untuk nilai: " + actualToken.getValue());
        assertEquals(expectedValue, actualToken.getValue(),
            "Nilai (lexeme) token salah. Diharapkan: " + expectedValue + ", Aktual: " + actualToken.getValue() +
            " untuk tipe: " + expectedType);
    }

    // --- Test Case 1: Kueri SELECT Dasar ---
    @Test
    void tokenize_BasicSelectQuery_ShouldProduceCorrectTokens() {
        String sql = "SELECT id, name FROM users WHERE id = 1;";

        List<Token> tokens = tokenize(sql);

        // Jumlah token: 12 (SELECT, id, COMMA, name, FROM, users, WHERE, id, OPERATOR, 1, SEMICOLON, EOF)
        assertEquals(12, tokens.size(), "Jumlah token harus sesuai.");

        assertToken(TokenType.SELECT, "SELECT", tokens.get(0));
        assertToken(TokenType.IDENTIFIER, "id", tokens.get(1));
        assertToken(TokenType.COMMA, ",", tokens.get(2));
        assertToken(TokenType.IDENTIFIER, "name", tokens.get(3));
        assertToken(TokenType.FROM, "FROM", tokens.get(4));
        assertToken(TokenType.IDENTIFIER, "users", tokens.get(5));
        assertToken(TokenType.WHERE, "WHERE", tokens.get(6));
        assertToken(TokenType.IDENTIFIER, "id", tokens.get(7));
        assertToken(TokenType.OPERATOR, "=", tokens.get(8));
        assertToken(TokenType.NUMBER_LITERAL, "1", tokens.get(9));
        assertToken(TokenType.SEMICOLON, ";", tokens.get(10));
        assertToken(TokenType.EOF, null, tokens.get(11));
    }

    // --- Test Case 2: Kueri JOIN dan AS dengan String Literal ---
    @Test
    void tokenize_JoinWithAliasAndLiteral_ShouldProduceCorrectTokens() {
        String sql = "SELECT T1.* AS data FROM table1 AS T1 JOIN table2 ON T1.id = 'abc';";

        List<Token> tokens = tokenize(sql);

        assertToken(TokenType.SELECT, "SELECT", tokens.get(0));
        assertToken(TokenType.IDENTIFIER, "T1", tokens.get(1));
        assertToken(TokenType.DOT, ".", tokens.get(2));
        assertToken(TokenType.STAR, "*", tokens.get(3));
        assertToken(TokenType.AS, "AS", tokens.get(4));
        assertToken(TokenType.IDENTIFIER, "data", tokens.get(5));
        assertToken(TokenType.FROM, "FROM", tokens.get(6));
        assertToken(TokenType.IDENTIFIER, "table1", tokens.get(7));
        assertToken(TokenType.AS, "AS", tokens.get(8));
        assertToken(TokenType.IDENTIFIER, "T1", tokens.get(9));
        assertToken(TokenType.JOIN, "JOIN", tokens.get(10));
        assertToken(TokenType.IDENTIFIER, "table2", tokens.get(11));
        assertToken(TokenType.ON, "ON", tokens.get(12));
        assertToken(TokenType.IDENTIFIER, "T1", tokens.get(13));
        assertToken(TokenType.DOT, ".", tokens.get(14));
        assertToken(TokenType.IDENTIFIER, "id", tokens.get(15));
        assertToken(TokenType.OPERATOR, "=", tokens.get(16));
        assertToken(TokenType.STRING_LITERAL, "'abc'", tokens.get(17));
        assertToken(TokenType.SEMICOLON, ";", tokens.get(18));
        assertToken(TokenType.EOF, null, tokens.get(19));
    }

    // --- Test Case 3: Keywords Baru (Transaction & Boolean) ---
    @Test
    void tokenize_TransactionAndBooleanKeywords_ShouldProduceCorrectTokens() {
        String sql = "BEGIN TRANSACTION; UPDATE users SET active = NOT active WHERE (col1 > 10 AND col2 OR false); COMMIT;";

        List<Token> tokens = tokenize(sql);

        // BEGIN TRANSACTION
        assertToken(TokenType.BEGIN, "BEGIN", tokens.get(0));
        assertToken(TokenType.TRANSACTION, "TRANSACTION", tokens.get(1));
        assertToken(TokenType.SEMICOLON, ";", tokens.get(2));

        // UPDATE
        assertToken(TokenType.UPDATE, "UPDATE", tokens.get(3));
        assertToken(TokenType.IDENTIFIER, "users", tokens.get(4));
        assertToken(TokenType.SET, "SET", tokens.get(5));
        assertToken(TokenType.IDENTIFIER, "active", tokens.get(6));
        assertToken(TokenType.OPERATOR, "=", tokens.get(7));
        assertToken(TokenType.NOT, "NOT", tokens.get(8));
        assertToken(TokenType.IDENTIFIER, "active", tokens.get(9));

        // WHERE (col1 > 10 AND col2 OR false)
        assertToken(TokenType.WHERE, "WHERE", tokens.get(10));
        assertToken(TokenType.LPARENTHESIS, "(", tokens.get(11));
        assertToken(TokenType.IDENTIFIER, "col1", tokens.get(12));
        assertToken(TokenType.OPERATOR, ">", tokens.get(13));
        assertToken(TokenType.NUMBER_LITERAL, "10", tokens.get(14));
        assertToken(TokenType.AND, "AND", tokens.get(15));
        assertToken(TokenType.IDENTIFIER, "col2", tokens.get(16));
        assertToken(TokenType.OR, "OR", tokens.get(17));
        assertToken(TokenType.FALSE, "false", tokens.get(18));
        assertToken(TokenType.RPARENTHESIS, ")", tokens.get(19));
        assertToken(TokenType.SEMICOLON, ";", tokens.get(20));

        // COMMIT
        assertToken(TokenType.COMMIT, "COMMIT", tokens.get(21));
        assertToken(TokenType.SEMICOLON, ";", tokens.get(22));
        assertToken(TokenType.EOF, null, tokens.get(23));
    }

    // --- Test Case 4: Gabungan Keywords Lainnya ---
    @Test
    void tokenize_ComplexStructure_ShouldProduceCorrectTokens() {
        String sql = "INSERT INTO table1 VALUES ('data', 20) DELETE FROM temp WHERE 1=1;";

        List<Token> tokens = tokenize(sql);

        // INSERT INTO table1 VALUES ('data', 20)
        assertToken(TokenType.INSERT, "INSERT", tokens.get(0));
        assertToken(TokenType.INTO, "INTO", tokens.get(1));
        assertToken(TokenType.IDENTIFIER, "table1", tokens.get(2));
        assertToken(TokenType.VALUES, "VALUES", tokens.get(3));
        assertToken(TokenType.LPARENTHESIS, "(", tokens.get(4));
        assertToken(TokenType.STRING_LITERAL, "'data'", tokens.get(5));
        assertToken(TokenType.COMMA, ",", tokens.get(6));
        assertToken(TokenType.NUMBER_LITERAL, "20", tokens.get(7));
        assertToken(TokenType.RPARENTHESIS, ")", tokens.get(8));

        // DELETE FROM temp WHERE 1=1 ORDER BY id LIMIT 5
        assertToken(TokenType.DELETE, "DELETE", tokens.get(9));
        assertToken(TokenType.FROM, "FROM", tokens.get(10));
        assertToken(TokenType.IDENTIFIER, "temp", tokens.get(11));
        assertToken(TokenType.WHERE, "WHERE", tokens.get(12));
        assertToken(TokenType.NUMBER_LITERAL, "1", tokens.get(13));
        assertToken(TokenType.OPERATOR, "=", tokens.get(14));
        assertToken(TokenType.NUMBER_LITERAL, "1", tokens.get(15));
        assertToken(TokenType.SEMICOLON, ";", tokens.get(16));
        assertToken(TokenType.EOF, null, tokens.get(17));
    }
}