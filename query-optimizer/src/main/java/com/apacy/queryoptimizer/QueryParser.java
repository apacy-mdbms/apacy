package com.apacy.queryoptimizer;

import java.text.ParseException;
import java.util.List;

import com.apacy.common.dto.ParsedQuery;
import com.apacy.queryoptimizer.parser.AbstractParser;
import com.apacy.queryoptimizer.parser.DDLParser;
import com.apacy.queryoptimizer.parser.DeleteParser;
import com.apacy.queryoptimizer.parser.InsertParser;
import com.apacy.queryoptimizer.parser.SelectParser;
import com.apacy.queryoptimizer.parser.TCLParser;
import com.apacy.queryoptimizer.parser.Token;
import com.apacy.queryoptimizer.parser.UpdateParser;

/**
 * SQL Query Parser that converts SQL strings into ParsedQuery objects.
 */
public class QueryParser {

    /**
     * Parse a SQL query string into a ParsedQuery object.
     */
    public ParsedQuery parse(String sqlQuery) throws ParseException {
        QueryTokenizer tokenizer = new QueryTokenizer(sqlQuery);
        List<Token> tokens = tokenizer.tokenize();

        AbstractParser parser = null;
        switch (tokens.get(0).getType()) {
            case SELECT:
                parser = new SelectParser(tokens);
                break;
            case UPDATE:
                parser = new UpdateParser(tokens);
                break;
            case INSERT:
                parser = new InsertParser(tokens);
                break;
            case DELETE:
                parser = new DeleteParser(tokens);
                break;
            case BEGIN:
            case ABORT:
            case COMMIT:
                parser = new TCLParser(tokens);
                break;
            case CREATE:
            case DROP:
                parser = new DDLParser(tokens);
                break;
            default:
                throw new UnsupportedOperationException("only SELECT, UPDATE, INSERT, and DELETE are currently supported");
        }

        return parser.parse();
    }

    /**
     * Validate a SQL query without full parsing.
     */
    public boolean validate(String sqlQuery) {
        QueryTokenizer tokenizer = new QueryTokenizer(sqlQuery);
        List<Token> tokens = tokenizer.tokenize();

        AbstractParser parser = null;
        switch (tokens.get(0).getType()) {
            case SELECT:
                parser = new SelectParser(tokens);
                break;
            case UPDATE:
                parser = new UpdateParser(tokens);
                break;
            case INSERT:
                parser = new InsertParser(tokens);
                break;
            case DELETE:
                parser = new DeleteParser(tokens);
                break;
            default:
                throw new UnsupportedOperationException("only SELECT, UPDATE, INSERT, and DELETE are currently supported");
        }

        return parser.validate();
    }

}