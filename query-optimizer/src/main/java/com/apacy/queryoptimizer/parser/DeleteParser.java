package com.apacy.queryoptimizer.parser;

import java.util.List;

import com.apacy.common.dto.ParsedQuery;

/**
 * Parser for DELETE query
 */
public class DeleteParser extends AbstractParser {

    public DeleteParser(List<Token> tokens) {
        super(tokens);
    }

    public ParsedQuery parse() {
        // TODO: parse DELETE query and return ParsedQuery object
        throw new UnsupportedOperationException("parse() for DELETE not implemented yet");
    };
}
