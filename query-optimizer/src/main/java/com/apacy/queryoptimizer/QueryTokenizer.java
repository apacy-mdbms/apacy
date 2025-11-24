package com.apacy.queryoptimizer;

import java.util.ArrayList;
import java.util.List;

import com.apacy.queryoptimizer.parser.Token;
import com.apacy.queryoptimizer.parser.TokenType;

/**
 * SQL query tokenizer
 */
public class QueryTokenizer {

    private final String input;
    private int pos = 0;
    private final int length;

    public QueryTokenizer(String input) {
        this.input = input;
        this.length = input.length();
    }

    public List<Token> tokenize() {
        List<Token> tokens = new ArrayList<>();

        while (pos < length) {
            char c = peek();

            if (Character.isWhitespace(c)) {
                advance();
                continue;
            }

            if (Character.isLetter(c)) {
                tokens.add(readWord());
            } else if (Character.isDigit(c)) {
                tokens.add(readNumber());
            } else if (c == '\'') {
                tokens.add(readString());
            } else {
                tokens.add(readSymbol());
            }
        }

        tokens.add(new Token(TokenType.EOF, null));
        return tokens;
    }

    private Token readWord() {
        int start = pos;
        while (pos < length && (Character.isLetterOrDigit(peek()) || peek() == '_' || peek() == '.'))
            advance();

        String word = input.substring(start, pos);
        String wordUpperCase = word.toUpperCase();
        TokenType type = null;
        try {
            type = TokenType.valueOf(wordUpperCase);
        } catch (IllegalArgumentException e) {
            type = TokenType.IDENTIFIER;
        }
        return new Token(type, word);
    }

    private Token readNumber() {
        int start = pos;
        while (pos < length && (Character.isDigit(peek()) || peek() == '.'))
            advance();

        return new Token(TokenType.NUMBER_LITERAL, input.substring(start, pos));
    }

    private Token readString() {
        advance(); // skip '
        int start = pos;
        while (pos < length && peek() != '\'')
            advance();

        String value = input.substring(start, pos);
        advance(); // skip '
        return new Token(TokenType.STRING_LITERAL, value);
    }

    private Token readSymbol() {
        char c = advance();
        switch (c) {
            case '.' -> { return new Token(TokenType.DOT, "."); }
            case ',' -> { return new Token(TokenType.COMMA, ","); }
            case ';' -> { return new Token(TokenType.SEMICOLON, ";"); }
            case '*' -> { return new Token(TokenType.STAR, "*"); }
            case '(' -> { return new Token(TokenType.LPARENTHESIS, "("); }
            case ')' -> { return new Token(TokenType.RPARENTHESIS, ")"); }
            case '=' -> { return new Token(TokenType.OPERATOR, "="); }
            case '>' -> {
                if (match('=')) return new Token(TokenType.OPERATOR, ">=");
                return new Token(TokenType.OPERATOR, ">");
            }
            case '<' -> {
                if (match('=')) return new Token(TokenType.OPERATOR, "<=");
                if (match('>')) return new Token(TokenType.OPERATOR, "<>");
                return new Token(TokenType.OPERATOR, "<");
            }
            default -> throw new RuntimeException("Unexpected character: " + c);
        }
    }

    private char peek() {
        return input.charAt(pos);
    }

    private char advance() {
        return input.charAt(pos++);
    }

    private boolean match(char expected) {
        if (pos < length && input.charAt(pos) == expected) {
            pos++;
            return true;
        }
        return false;
    }
}
