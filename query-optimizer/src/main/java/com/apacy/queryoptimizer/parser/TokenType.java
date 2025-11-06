package com.apacy.queryoptimizer.parser;

public enum TokenType {
    // keywords
    SELECT, FROM, WHERE, INSERT, UPDATE, DELETE, INTO, VALUES, SET,

    // symbols
    OPERATOR, LPARENTHESIS, RPARENTHESIS,
    COMMA, SEMICOLON, STAR,

    // Literals
    IDENTIFIER, STRING_LITERAL, NUMBER_LITERAL,

    // End of input
    EOF
}