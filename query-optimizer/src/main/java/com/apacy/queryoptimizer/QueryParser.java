package com.apacy.queryoptimizer;

import java.util.List;

import com.apacy.common.dto.ParsedQuery;
import com.apacy.queryoptimizer.parser.DeleteParser;
import com.apacy.queryoptimizer.parser.InsertParser;
import com.apacy.queryoptimizer.parser.SelectParser;
import com.apacy.queryoptimizer.parser.Token;
import com.apacy.queryoptimizer.parser.TokenType;
import com.apacy.queryoptimizer.parser.UpdateParser;

/**
 * SQL Query Parser that converts SQL strings into ParsedQuery objects.
 * Berfungsi sebagai "dispatcher" yang menjalankan tokenizer dan
 * memilih parser yang tepat (Select, Insert, Update, Delete).
 */
public class QueryParser {

    /**
     * Parse a SQL query string into a ParsedQuery object.
     */
    public ParsedQuery parse(String sqlQuery) {
        // 1. Jalankan Tokenizer
        QueryTokenizer tokenizer = new QueryTokenizer(sqlQuery);
        List<Token> tokens = tokenizer.tokenize();

        if (tokens.isEmpty() || tokens.get(0).getType() == TokenType.EOF) {
            throw new RuntimeException("Query kosong.");
        }

        // 2. Tentukan parser berdasarkan token pertama
        TokenType firstToken = tokens.get(0).getType();

        switch (firstToken) {
            case INSERT:
                // Panggil kelas Anda!
                return new InsertParser(tokens).parse();
            
            case SELECT:
                return new SelectParser(tokens).parse();
                
            case UPDATE:
                return new UpdateParser(tokens).parse();
                
            case DELETE:
                return new DeleteParser(tokens).parse();
                
            default:
                throw new RuntimeException("Tipe query tidak didukung: " + firstToken);
        }
    }

    /**
     * Validate a SQL query without full parsing.
     * TODO: Implement syntax validation without full AST construction
     */
    public boolean validate(String sqlQuery) {
        // TODO: Implement query validation logic
        throw new UnsupportedOperationException("validate not implemented yet");
    }
}