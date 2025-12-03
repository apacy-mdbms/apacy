package com.apacy.queryoptimizer.parser;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import com.apacy.common.dto.ParsedQuery;
import com.apacy.common.dto.ast.expression.ColumnFactor;
import com.apacy.common.dto.ast.expression.ExpressionNode;
import com.apacy.common.dto.ast.expression.FactorNode;
import com.apacy.common.dto.ast.expression.LiteralFactor;
import com.apacy.common.dto.ast.expression.TermNode;
import com.apacy.common.dto.ast.join.JoinConditionNode;
import com.apacy.common.dto.ast.join.JoinOperand;
import com.apacy.common.dto.ast.join.TableNode;
import com.apacy.common.dto.ast.where.BinaryConditionNode;
import com.apacy.common.dto.ast.where.ComparisonConditionNode;
import com.apacy.common.dto.ast.where.UnaryConditionNode;
import com.apacy.common.dto.ast.where.WhereConditionNode;
import com.apacy.common.dto.plan.CartesianNode;
import com.apacy.common.dto.plan.FilterNode;
import com.apacy.common.dto.plan.JoinNode;
import com.apacy.common.dto.plan.PlanNode;
import com.apacy.common.dto.plan.ProjectNode;
import com.apacy.common.dto.plan.ScanNode;

/**
 * Base parser class for any query parser
 * Contains basic utilites and the abstract parse() function to be implemented by inheriting classes
 */
public abstract class AbstractParser {

    protected final List<Token> tokens;
    protected int position = 0;

    public AbstractParser(List<Token> tokens) {
        this.tokens = tokens;
    }

    /**
     * Parse query and return the ParsedQuery object
     * Throws ParseException if fail to parse.
     */
    public abstract ParsedQuery parse() throws ParseException;

    /**
     * Parse and validate query syntax, without creating ParsedQuery object
     */
    public abstract boolean validate();


    /**
     * peek and get token in current position.
     */
    protected Token peek() {
        return position < tokens.size() ? tokens.get(position) : new Token(TokenType.EOF, null);
    }

    /**
     * match token in current position.
     */
    protected boolean match(TokenType type) {
        if (peek().getType() == type) {
            position++;
            return true;
        }
        return false;
    }

    /**
     * Enforce expected token type and get token in current position
     * Throws error if current token type != expected token type
     */
    protected Token consume(TokenType expected) {
        Token token = peek();
        if (token.getType() != expected) {
            throw new RuntimeException("Expected " + expected + " but found " + token.getType().toString());
        }
        position++;
        return token;
    }

    /* Parse WHERE (untuk SELECT/INSERT/UPDATE/DELETE)
     *  <where> ::= <or>
     *  <or> ::= <and>
     *  <and> ::= <unary>
     *  <unary> ::= 'NOT' <primary>
     *  <primary> ::= '(' <where> ')' | <comparison>
     *  <comparison> ::= <expression> <comparison_op> <expression>
     *      comparison_op = { =, <, >, <=, >=, <>, dll }
     *  <expression> ::= <term> { ('+' | '-') <term> }
     *  <term> ::= <factor> { ('*' | '/' | '%') <factor> }
     *  <factor> ::= <column> | <literal> | '(' <expression> ')'
     */

    protected WhereConditionNode parseWhereExpression() {
        return parseOr();
    }

    protected WhereConditionNode parseOr() {
        WhereConditionNode left = parseAnd();
        while (match(TokenType.OR)) {
            WhereConditionNode right = parseAnd();
            left = new BinaryConditionNode(left, "OR", right);
        }
        return left;
    }

    protected WhereConditionNode parseAnd() {
        WhereConditionNode left = parseUnary();
        while (match(TokenType.AND)) {
            WhereConditionNode right = parseUnary();
            left = new BinaryConditionNode(left, "AND", right);
        }
        return left;
    }

    protected WhereConditionNode parseUnary() {
        if (match(TokenType.NOT)) {
            WhereConditionNode operand = parsePrimaryCondition();
            return new UnaryConditionNode("NOT", operand);
        }
        return parsePrimaryCondition();
    }

    // 2 kemungkinan untuk sebuah kondisi primary:
    protected WhereConditionNode parsePrimaryCondition() {
        // 1. '(' <where_condition> ')'
        if (peek().getType() == TokenType.LPARENTHESIS) {
            int saved = position;
            try {
                consume(TokenType.LPARENTHESIS);
                WhereConditionNode inner = parseWhereExpression();
                consume(TokenType.RPARENTHESIS);
                return inner;
            } catch (RuntimeException ex) {
                // Backtrack, coba sebagai kemungkinan ke-2 (cth `(a-1) <= b`)
                position = saved;
            }
        }

        // 2. <expression> <comparison_op> <expression>
        ExpressionNode leftExpr = parseExpression();
        String operator = consume(TokenType.OPERATOR).getValue();
        ExpressionNode rightExpr = parseExpression();
        return new ComparisonConditionNode(leftExpr, operator, rightExpr);
    }

    protected boolean validateWhere() {
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
    protected ExpressionNode parseExpression() {
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
    protected TermNode parseTerm() {
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
    protected FactorNode parseFactor() {
        Token t = peek();
        if (t.getType() == TokenType.IDENTIFIER) {
            String f = consume(TokenType.IDENTIFIER).getValue();
            while(match(TokenType.DOT)) {
                Token t2 = peek();
                if (match(TokenType.IDENTIFIER)) {
                    f += '.' + t2.getValue();
                } else if (match(TokenType.STAR)) {
                    f += ".*";
                } else { consume(TokenType.IDENTIFIER);} //throw error
            }
            return new ColumnFactor(f);
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
    protected boolean matchSymbol(String symbol) {
        Token t = peek();
        if (t.getValue() != null && t.getValue().equals(symbol)) {
            position++;
            return true;
        }
        return false;
    }

    // Fungsi utk convert string ke integer,
    //  atau double kalau tdk bisa integer,
    //  atau tetap string kalau tidak bisa double
    protected Object parseNumberLiteral(String value) {
        try { return Integer.parseInt(value); } catch (NumberFormatException e) {}
        try { return Double.parseDouble(value); } catch (NumberFormatException e) {}
        return value;
    }


    protected PlanNode generatePlanNode(JoinOperand joinCondition, WhereConditionNode whereCondition, List<String> targetColumns) {
        PlanNode fromTree = buildJoinTree(joinCondition);

        if (whereCondition != null)
            fromTree = new FilterNode(fromTree, whereCondition);

        if (targetColumns != null && !targetColumns.isEmpty())
            fromTree = new ProjectNode(fromTree, targetColumns);


        return fromTree;
    }

    protected PlanNode buildJoinTree(JoinOperand node) {
        if (node == null) return null;

        if (node instanceof TableNode t) {
            return new ScanNode(t.tableName(), "");
        } else {

            JoinConditionNode j = (JoinConditionNode)node;

            PlanNode left = buildJoinTree(j.left());
            PlanNode right = buildJoinTree(j.right());
            if (j.joinType().equalsIgnoreCase("CROSS")) {
                return new CartesianNode(left, right);
            }
            return new JoinNode(left, right, j.conditions(), j.joinType());
        }

    }

}
