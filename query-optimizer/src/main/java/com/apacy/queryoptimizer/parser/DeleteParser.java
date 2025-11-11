package com.apacy.queryoptimizer.parser;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import com.apacy.common.dto.ParsedQuery;
import com.apacy.queryoptimizer.ast.expression.ColumnFactor;
import com.apacy.queryoptimizer.ast.expression.ExpressionNode;
import com.apacy.queryoptimizer.ast.expression.FactorNode;
import com.apacy.queryoptimizer.ast.expression.LiteralFactor;
import com.apacy.queryoptimizer.ast.expression.TermNode;
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
        
        return new ParsedQuery("DELETE", targetTables, null,
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

    // 2 kemungkinan untuk sebuah kondisi primary:
    private WhereConditionNode parsePrimary() {
        // 1. '(' <where_condition> ')'
        if (peek().getType() == TokenType.LPARENTHESIS) {
            int saved = position;
            try {
                consume(TokenType.LPARENTHESIS);
                WhereConditionNode inner = parseWhereExpression();
                consume(TokenType.RPARENTHESIS);
                return inner;
            } catch (RuntimeException ex) {
                // Backtrack, coba sebagai kemungkinan ke-2 (cth `(a+1) > b`)
                position = saved;
            }
        }

        // 2. <expression> <comparison_op> <expression>
        ExpressionNode leftExpr = parseExpression();
        String operator = consume(TokenType.OPERATOR).getValue();
        ExpressionNode rightExpr = parseExpression();
        return new ComparisonConditionNode(leftExpr, operator, rightExpr);
    }

    private Object parseNumberLiteral(String value) {
        try { return Integer.parseInt(value); } catch (NumberFormatException e) {}
        try { return Double.parseDouble(value); } catch (NumberFormatException e) {}
        return value;
    }

    private boolean validateWhere() {
        int saved = position;
        try {
            parseWhereExpression();
            return true;
        } catch (Exception e) {
            position = saved;
            return false;
        }
    }

    /* Grammar ComparisonConditionNode) */

    // <expression> ::= <term> { ('+' | '-') <term> }
    private ExpressionNode parseExpression() {
        TermNode first = parseTerm();
        List<ExpressionNode.TermPair> remainder = new ArrayList<>();
        while (true) {
            if (matchSymbol("+")) {
                TermNode t = parseTerm();
                remainder.add(new ExpressionNode.TermPair('+', t));
            } else if (matchSymbol("-")) {
                TermNode t = parseTerm();
                remainder.add(new ExpressionNode.TermPair('-', t));
            } else {
                break;
            }
        }
        return new ExpressionNode(first, remainder);
    }

    // <term> ::= <factor> { ('*' | '/' | '%') <factor> }
    private TermNode parseTerm() {
        FactorNode first = parseFactor();
        List<TermNode.FactorPair> remainder = new ArrayList<>();
        while (true) {
            if (matchSymbol("*")) {
                FactorNode f = parseFactor();
                remainder.add(new TermNode.FactorPair('*', f));
            } else if (matchSymbol("/")) {
                FactorNode f = parseFactor();
                remainder.add(new TermNode.FactorPair('/', f));
            } else if (matchSymbol("%")) {
                FactorNode f = parseFactor();
                remainder.add(new TermNode.FactorPair('%', f));
            } else {
                break;
            }
        }
        return new TermNode(first, remainder);
    }

    // <factor> ::= <column> | <literal> | '(' <expression> ')'
    private FactorNode parseFactor() {
        Token t = peek();
        if (t.getType() == TokenType.IDENTIFIER) {
            position++;
            return new ColumnFactor(t.getValue());
        } else if (t.getType() == TokenType.NUMBER_LITERAL) {
            position++;
            return new LiteralFactor(parseNumberLiteral(t.getValue()));
        } else if (t.getType() == TokenType.STRING_LITERAL) {
            position++;
            return new LiteralFactor(t.getValue());
        } else if (match(TokenType.LPARENTHESIS)) {
            ExpressionNode inner = parseExpression();
            consume(TokenType.RPARENTHESIS);
            return inner;
        } else {
            throw new RuntimeException("Unexpected token in expression: " + t.getType());
        }
    }

    // Fungsi utk bantu match simbol operator 1 character berdasarkan nilai token
    private boolean matchSymbol(String symbol) {
        Token t = peek();
        if (t.getValue() != null && t.getValue().equals(symbol)) {
            position++;
            return true;
        }
        return false;
    }
}
