package com.apacy.queryoptimizer.parser;

import java.util.List;

import com.apacy.common.dto.ParsedQuery;

/**
 * Base parser class for any query parser
 * Contains basic utilites and the abstract parse() function to be implemented by inheriting classes
 */
public abstract class AbstractParser {

    protected final List<Token> tokens;
    protected int position = 0;

    public AbstractParser(List<Token> tokens) {
        this.tokens = tokens;
    }

    /**
     * peek and get token in current position.
     */
    protected Token peek() {
        return position < tokens.size() ? tokens.get(position) : new Token(TokenType.EOF, "");
    }

    /**
     * match token in current position.
     */
    protected boolean match(TokenType type) {
        if (peek().getType() == type) {
            position++;
            return true;
        }
        return false;
    }

    /**
     * Enforce expected token type and get token in current position
     * Throws error if current token type != expected token type
     */
    protected Token consume(TokenType expected) {
        Token token = peek();
        if (token.getType() != expected) {
            throw new RuntimeException("Expected " + expected + " but found " + token.getType().toString());
        }
        position++;
        return token;
    }

    /**
     * Parse query and return the ParsedQuery object
     */
    public abstract ParsedQuery parse();
}
