package com.apacy.queryoptimizer.parser;

import java.util.List;

import com.apacy.common.dto.ParsedQuery;

/**
 * Parser for UPDATE query
 */
public class UpdateParser extends AbstractParser {

    public UpdateParser(List<Token> tokens) {
        super(tokens);
    }

    public ParsedQuery parse() {
        // TODO: parse UPDATE query and return ParsedQuery object
        throw new UnsupportedOperationException("parse() for UPDATE not implemented yet");
    };
}
