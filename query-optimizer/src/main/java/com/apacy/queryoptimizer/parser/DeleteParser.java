package com.apacy.queryoptimizer.parser;

import java.text.ParseException;
import java.util.List;

import com.apacy.common.dto.ParsedQuery;

/**
 * Parser for DELETE query
 */
public class DeleteParser extends AbstractParser {

    public DeleteParser(List<Token> tokens) {
        super(tokens);
    }

    @Override
    public ParsedQuery parse() throws ParseException {
        // TODO: parse DELETE query and return ParsedQuery object
        throw new UnsupportedOperationException("parse() for DELETE not implemented yet");
    };

    @Override
    public boolean validate() {
        // TODO: parse DELETE query and validate syntax
        throw new UnsupportedOperationException("validate() for DELETE not implemented yet");
    }
}
