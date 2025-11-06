package com.apacy.queryoptimizer.parser;

import java.util.List;

import com.apacy.common.dto.ParsedQuery;

/**
 * Parser for INSERT query
 */
public class InsertParser extends AbstractParser {

    public InsertParser(List<Token> tokens) {
        super(tokens);
    }

    public ParsedQuery parse() {
        // TODO: parse INSERT query and return ParsedQuery object
        throw new UnsupportedOperationException("parse() for INSERT not implemented yet");
    };
}
