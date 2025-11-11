package com.apacy.queryoptimizer.parser;

public enum TokenType {
    // keywords
    SELECT, FROM, WHERE, INSERT, UPDATE, DELETE, INTO, VALUES, SET,
    JOIN, ON, NATURAL, ORDER, BY, LIMIT, CREATE, TABLE, DROP, AS,
    BEGIN, TRANSACTION, COMMIT,
    AND, OR, NOT,
    FALSE, TRUE,

    // symbols
    OPERATOR, LPARENTHESIS, RPARENTHESIS,
    COMMA, SEMICOLON, STAR, DOT,

    // Literals
    IDENTIFIER, STRING_LITERAL, NUMBER_LITERAL,

    // End of input
    EOF
}