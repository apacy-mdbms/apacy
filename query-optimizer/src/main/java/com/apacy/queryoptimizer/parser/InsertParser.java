package com.apacy.queryoptimizer.parser;

import java.text.ParseException;
import java.util.List;

import com.apacy.common.dto.ParsedQuery;

/**
 * Parser for INSERT query
 */
public class InsertParser extends AbstractParser {

    public InsertParser(List<Token> tokens) {
        super(tokens);
    }

    @Override
    public ParsedQuery parse() throws ParseException {
        // TODO: parse INSERT query and return ParsedQuery object
        throw new UnsupportedOperationException("parse() for INSERT not implemented yet");
    };

    @Override
    public boolean validate() {
        // TODO: parse INSERT query and validate syntax
        throw new UnsupportedOperationException("validate() for INSERT not implemented yet");
    }
}
