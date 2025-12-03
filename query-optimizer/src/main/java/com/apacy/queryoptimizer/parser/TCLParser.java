package com.apacy.queryoptimizer.parser;

import java.text.ParseException;
import java.util.List;

import com.apacy.common.dto.ParsedQuery;
import com.apacy.common.dto.plan.TCLNode;

/**
 * Parser for TCL query
 */
public class TCLParser extends AbstractParser {

    public TCLParser(List<Token> tokens) {
        super(tokens);
    }

    @Override
    public ParsedQuery parse() throws ParseException {
        if(match(TokenType.BEGIN)) {
            consume(TokenType.TRANSACTION);
            return new ParsedQuery(
            "BEGIN",
            new TCLNode("BEGIN"),
            null,
            null,
            null,
            null,
            null,
            null,
            false,
            false);
        }
        if(match(TokenType.COMMIT)) {
            return new ParsedQuery(
            "COMMIT",
            new TCLNode("COMMIT"),
            null,
            null,
            null,
            null,
            null,
            null,
            false,
            false);
        }
        if(match(TokenType.ABORT)) {
            return new ParsedQuery(
            "ABORT",
            new TCLNode("ABORT"),
            null,
            null,
            null,
            null,
            null,
            null,
            false,
            false);
        }

        throw new ParseException("Supported TCL keywords are BEGIN TRANSACTION, COMMIT, and ABORT", position);
    };

    @Override
    public boolean validate() {
        return match(TokenType.COMMIT) || match(TokenType.ABORT) || (match(TokenType.BEGIN) && match(TokenType.TRANSACTION));

    }
}

