package com.apacy.queryoptimizer.parser;

/**
 * Token class for lexer
 */
 public class Token {

    private final TokenType type;
    private final String value;

    public Token(TokenType type, String value) {
        this.type = type;
        this.value = value;
    }

    public TokenType getType() {
        return this.type;
    }

    public String getValue() {
        return this.value;
    }
}