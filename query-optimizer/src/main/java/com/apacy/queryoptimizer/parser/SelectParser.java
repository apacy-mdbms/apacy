package com.apacy.queryoptimizer.parser;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import com.apacy.common.dto.ParsedQuery;
import com.apacy.queryoptimizer.ast.join.JoinConditionNode;
import com.apacy.queryoptimizer.ast.join.JoinOperand;
import com.apacy.queryoptimizer.ast.join.TableNode;
import com.apacy.queryoptimizer.ast.where.BinaryConditionNode;
import com.apacy.queryoptimizer.ast.where.ComparisonConditionNode;
import com.apacy.queryoptimizer.ast.where.UnaryConditionNode;
import com.apacy.queryoptimizer.ast.where.WhereConditionNode;

public class SelectParser extends AbstractParser {

    public SelectParser(List<Token> tokens) {
        super(tokens);
    }

    @Override
    public ParsedQuery parse() throws ParseException {
        consume(TokenType.SELECT);

        List<String> targetColumns = new ArrayList<>();
        if (match(TokenType.STAR)) {
            targetColumns.add("*");
        } else {
            targetColumns.add(consume(TokenType.IDENTIFIER).getValue());
            while (match(TokenType.COMMA)) {
                targetColumns.add(consume(TokenType.IDENTIFIER).getValue());
            }
        }

        consume(TokenType.FROM);

        List<String> targetTables = new ArrayList<>();
        JoinOperand joinAst = null;

        Token firstTableToken = consume(TokenType.IDENTIFIER);
        targetTables.add(firstTableToken.getValue());
        TableNode leftTable = new TableNode(firstTableToken.getValue());

        while (match(TokenType.JOIN)) {
            Token rightTableToken = consume(TokenType.IDENTIFIER);
            TableNode rightTable = new TableNode(rightTableToken.getValue());
            targetTables.add(rightTableToken.getValue());

            consume(TokenType.ON);
            String leftOperand = consume(TokenType.IDENTIFIER).getValue();
            String operator = consume(TokenType.OPERATOR).getValue();
            Token rightOpToken = consume(peek().getType());
            String rightOperand = rightOpToken.getValue();

            ComparisonConditionNode condition = new ComparisonConditionNode(leftOperand, operator, rightOperand);
            JoinConditionNode thisJoin = new JoinConditionNode("INNER", leftTable, rightTable, condition);

            if (joinAst == null) {
                joinAst = thisJoin;
            } else {
                joinAst = new JoinConditionNode("INNER", joinAst, rightTable, condition);
            }

            leftTable = (joinAst instanceof TableNode) ? (TableNode) joinAst : leftTable;
        }

        WhereConditionNode where = null;
        if (match(TokenType.WHERE)) {
            where = parseWhereExpression();
        }

        String orderBy = null;
        boolean isDesc = false;
        if (match(TokenType.ORDER)) {
            consume(TokenType.BY);
            orderBy = consume(TokenType.IDENTIFIER).getValue();
        }

        if (match(TokenType.LIMIT)) {
            consume(TokenType.NUMBER_LITERAL);
        }

        if (peek().getType() == TokenType.SEMICOLON) consume(TokenType.SEMICOLON);

        Object joinConditions = joinAst;
        Object whereClause = where;

        return new ParsedQuery("SELECT", targetTables, targetColumns, joinConditions, whereClause, orderBy, isDesc, false);
    };

    @Override
    public boolean validate() {
        try {
            int savedPos = position;
            if (!match(TokenType.SELECT)) return false;

            if (match(TokenType.STAR)) {
            } else {
                if (peek().getType() != TokenType.IDENTIFIER) { position = savedPos; return false; }
                position++;
                while (match(TokenType.COMMA)) {
                    if (peek().getType() != TokenType.IDENTIFIER) { position = savedPos; return false; }
                    position++;
                }
            }

            if (!match(TokenType.FROM)) { position = savedPos; return false; }

            if (peek().getType() != TokenType.IDENTIFIER) { position = savedPos; return false; }
            position++;

            while (match(TokenType.JOIN)) {
                if (peek().getType() != TokenType.IDENTIFIER) { position = savedPos; return false; }
                position++;
                if (!match(TokenType.ON)) { position = savedPos; return false; }
                if (peek().getType() != TokenType.IDENTIFIER) { position = savedPos; return false; }
                position++;
                if (peek().getType() != TokenType.OPERATOR) { position = savedPos; return false; }
                position++;
                if (peek().getType() != TokenType.IDENTIFIER && peek().getType() != TokenType.NUMBER_LITERAL) { position = savedPos; return false; }
                position++;
            }

            if (match(TokenType.WHERE)) {
                if (!validateWhere()) { position = savedPos; return false; }
            }

            if (match(TokenType.ORDER)) {
                if (!match(TokenType.BY)) { position = savedPos; return false; }
                if (peek().getType() != TokenType.IDENTIFIER) { position = savedPos; return false; }
                position++;
            }

            if (match(TokenType.LIMIT)) {
                if (peek().getType() != TokenType.NUMBER_LITERAL) { position = savedPos; return false; }
                position++;
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
