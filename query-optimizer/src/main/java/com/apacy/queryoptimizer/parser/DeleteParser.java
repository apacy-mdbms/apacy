package com.apacy.queryoptimizer.parser;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import com.apacy.common.dto.ParsedQuery;
import com.apacy.common.dto.ast.where.WhereConditionNode;
import com.apacy.common.dto.plan.PlanNode;

/**
 * Parser for DELETE query
 */
public class DeleteParser extends AbstractParser {

    public DeleteParser(List<Token> tokens) {
        super(tokens);
    }

    @Override
    public ParsedQuery parse() throws ParseException {
        // `DELETE FROM [TABLE NAME]`
        consume(TokenType.DELETE);
        consume(TokenType.FROM);
        Token tableToken = consume(TokenType.IDENTIFIER);
        List<String> targetTables = new ArrayList<>();
        targetTables.add(tableToken.getValue());

        // `DELETE FROM [TABLE NAME]
        // WHERE ...`
        WhereConditionNode where = null;
        if (match(TokenType.WHERE)) {
            where = parseWhereExpression();
        }

        // `DELETE FROM [TABLE NAME]
        // ...;`
        if (peek().getType() == TokenType.SEMICOLON) {
            consume(TokenType.SEMICOLON);
        }

        PlanNode planRoot = generatePlanNode(null, where, null);
        return new ParsedQuery("DELETE", planRoot, targetTables, null,
                                null, null, where,
                                null, false, false);
    };

    @Override
    public boolean validate() {
        try {
            int savedPos = position;
            if (!match(TokenType.DELETE)) return false;
            if (!match(TokenType.FROM)) { position = savedPos; return false; }
            if (peek().getType() != TokenType.IDENTIFIER) { position = savedPos; return false; }
            position++;

            if (match(TokenType.WHERE)) {
                if (!validateWhere()) { position = savedPos; return false; }
            }

            if (peek().getType() == TokenType.SEMICOLON) position++;

            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
