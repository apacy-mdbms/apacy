package com.apacy.queryoptimizer.parser;

import java.text.ParseException;
import java.util.List;

import com.apacy.common.dto.ParsedQuery;

/**
 * Parser for SELECT query
 */
public class SelectParser extends AbstractParser {

    public SelectParser(List<Token> tokens) {
        super(tokens);
    }

    @Override
    public ParsedQuery parse() throws ParseException {
        // TODO: parse SELECT query and return ParsedQuery object
        throw new UnsupportedOperationException("parse() for SELECT not implemented yet");
    };

    @Override
    public boolean validate() {
        // TODO: parse SELECT query and validate syntax
        throw new UnsupportedOperationException("validate() for SELECT not implemented yet");
    }
}
