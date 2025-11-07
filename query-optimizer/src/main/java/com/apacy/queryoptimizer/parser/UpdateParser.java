package com.apacy.queryoptimizer.parser;

import java.text.ParseException;
import java.util.List;

import com.apacy.common.dto.ParsedQuery;

/**
 * Parser for UPDATE query
 */
public class UpdateParser extends AbstractParser {

    public UpdateParser(List<Token> tokens) {
        super(tokens);
    }

    @Override
    public ParsedQuery parse() throws ParseException {
        // TODO: parse UPDATE query and return ParsedQuery object
        throw new UnsupportedOperationException("parse() for UPDATE not implemented yet");
    };

    @Override
    public boolean validate() {
        // TODO: parse UPDATE query and validate syntax
        throw new UnsupportedOperationException("validate() for UPDATE not implemented yet");
    }
}
