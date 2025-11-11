package com.apacy.queryoptimizer.parser;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import com.apacy.common.dto.ParsedQuery;
import com.apacy.queryoptimizer.ast.where.BinaryConditionNode;
import com.apacy.queryoptimizer.ast.where.ComparisonConditionNode;
import com.apacy.queryoptimizer.ast.where.UnaryConditionNode;
import com.apacy.queryoptimizer.ast.where.WhereConditionNode;

/**
 * Parser for DELETE query
 */
public class DeleteParser extends AbstractParser {

    public DeleteParser(List<Token> tokens) {
        super(tokens);
    }

    @Override
    public ParsedQuery parse() throws ParseException {
        consume(TokenType.DELETE);
        consume(TokenType.FROM);
        Token tableToken = consume(TokenType.IDENTIFIER);
        List<String> targetTables = new ArrayList<>();
        targetTables.add(tableToken.getValue());

        WhereConditionNode where = null;
        if (match(TokenType.WHERE)) {
            where = parseWhereExpression();
        }

        if (peek().getType() == TokenType.SEMICOLON) {
            consume(TokenType.SEMICOLON);
        }

        return new ParsedQuery("DELETE", targetTables, null, null, where, null, false, false);
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

    private WhereConditionNode parseWhereExpression() {
        return parseOr();
    }

    private WhereConditionNode parseOr() {
        WhereConditionNode left = parseAnd();
        while (match(TokenType.OR)) {
            WhereConditionNode right = parseAnd();
            left = new BinaryConditionNode(left, "OR", right);
        }
        return left;
    }

    private WhereConditionNode parseAnd() {
        WhereConditionNode left = parseUnary();
        while (match(TokenType.AND)) {
            WhereConditionNode right = parseUnary();
            left = new BinaryConditionNode(left, "AND", right);
        }
        return left;
    }

    private WhereConditionNode parseUnary() {
        if (match(TokenType.NOT)) {
            WhereConditionNode operand = parsePrimary();
            return new UnaryConditionNode("NOT", operand);
        }
        return parsePrimary();
    }

    private WhereConditionNode parsePrimary() {
        if (match(TokenType.LPARENTHESIS)) {
            WhereConditionNode inner = parseWhereExpression();
            consume(TokenType.RPARENTHESIS);
            return inner;
        }

        String left = consume(TokenType.IDENTIFIER).getValue();
        String operator = consume(TokenType.OPERATOR).getValue();
        Token rightTok = peek();
        if (rightTok.getType() == TokenType.NUMBER_LITERAL) {
            position++;
            return new ComparisonConditionNode(left, operator, parseNumberLiteral(rightTok.getValue()));
        } else if (rightTok.getType() == TokenType.STRING_LITERAL || rightTok.getType() == TokenType.IDENTIFIER) {
            position++;
            return new ComparisonConditionNode(left, operator, rightTok.getValue());
        } else {
            throw new RuntimeException("Invalid where clause token: " + rightTok.getType());
        }
    }

    private Object parseNumberLiteral(String value) {
        try { return Integer.parseInt(value); } catch (NumberFormatException e) {}
        try { return Double.parseDouble(value); } catch (NumberFormatException e) {}
        return value;
    }

    private boolean validateWhere() {
        int depth = 0;
        boolean expectOperand = true;
        while (position < tokens.size()) {
            Token t = peek();
            if (t.getType() == TokenType.IDENTIFIER) {
                position++;
                if (peek().getType() != TokenType.OPERATOR) return false;
                position++;
                if (peek().getType() != TokenType.NUMBER_LITERAL && peek().getType() != TokenType.IDENTIFIER && peek().getType() != TokenType.STRING_LITERAL) return false;
                position++;
                expectOperand = false;
            } else if (t.getType() == TokenType.AND || t.getType() == TokenType.OR) {
                position++;
                expectOperand = true;
            } else if (t.getType() == TokenType.LPARENTHESIS) {
                depth++; position++; expectOperand = true;
            } else if (t.getType() == TokenType.RPARENTHESIS) {
                depth--; position++; expectOperand = false;
                if (depth < 0) return false;
            } else break;
        }
        return depth == 0 && !expectOperand;
    }
}
