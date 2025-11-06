package com.apacy.queryoptimizer.parser;

import java.util.List;

import com.apacy.common.dto.ParsedQuery;

/**
 * Parser for SELECT query
 */
public class SelectParser extends AbstractParser {

    public SelectParser(List<Token> tokens) {
        super(tokens);
    }

    public ParsedQuery parse() {
        // TODO: parse SELECT query and return ParsedQuery object
        throw new UnsupportedOperationException("parse() for SELECT not implemented yet");
    };
}
