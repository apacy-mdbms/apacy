package com.apacy.queryoptimizer.parser;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import com.apacy.common.dto.ParsedQuery;
import com.apacy.common.dto.plan.PlanNode;
import com.apacy.queryoptimizer.ast.join.JoinConditionNode;
import com.apacy.queryoptimizer.ast.join.JoinOperand;
import com.apacy.queryoptimizer.ast.join.TableNode;
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
        JoinOperand joinAst = parseTableReferenceList(targetTables);

        // Token firstTableToken = consume(TokenType.IDENTIFIER);
        // targetTables.add(firstTableToken.getValue());
        // TableNode leftTable = new TableNode(firstTableToken.getValue());

        // while (true) {
        //     break;
        // }

        // while (match(TokenType.JOIN)) {
        //     Token rightTableToken = consume(TokenType.IDENTIFIER);
        //     TableNode rightTable = new TableNode(rightTableToken.getValue());
        //     targetTables.add(rightTableToken.getValue());

        //     consume(TokenType.ON);
        //     String leftOp = consume(TokenType.IDENTIFIER).getValue();
        //     String operator = consume(TokenType.OPERATOR).getValue();
        //     Token rightOpToken = peek();
        //     ExpressionNode leftExpr = new ExpressionNode(new TermNode(new ColumnFactor(leftOp), null), null);
        //     ExpressionNode rightExpr;
        //     if (rightOpToken.getType() == TokenType.IDENTIFIER) {
        //         position++;
        //         rightExpr = new ExpressionNode(new TermNode(new ColumnFactor(rightOpToken.getValue()), null), null);
        //     } else if (rightOpToken.getType() == TokenType.NUMBER_LITERAL) {
        //         position++;
        //         rightExpr = new ExpressionNode(new TermNode(new LiteralFactor(parseNumberLiteral(rightOpToken.getValue())), null), null);
        //     } else {
        //         position++;
        //         rightExpr = new ExpressionNode(new TermNode(new ColumnFactor(rightOpToken.getValue()), null), null);
        //     }

        //     ComparisonConditionNode condition = new ComparisonConditionNode(leftExpr, operator, rightExpr);
        //     JoinConditionNode thisJoin = new JoinConditionNode("INNER", leftTable, rightTable, condition);

        //     if (joinAst == null) {
        //         joinAst = thisJoin;
        //     } else {
        //         joinAst = new JoinConditionNode("INNER", joinAst, rightTable, condition);
        //     }

        //     leftTable = (joinAst instanceof TableNode) ? (TableNode) joinAst : leftTable;
        // }

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

        PlanNode planRoot = generatePlanNode((JoinOperand)joinAst, where, targetColumns);
        return new ParsedQuery("SELECT", planRoot, targetTables, targetColumns,
                (List<Object>) null, joinConditions, whereClause,
                orderBy, isDesc, false);
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

    protected JoinOperand parseTableReferenceList(List<String> targetTables) throws ParseException {
        JoinOperand ast = parseTableReference(targetTables);

        while(match(TokenType.COMMA)) {
            JoinOperand rightRef = parseTableReference(targetTables);
            ast = new JoinConditionNode("CROSS", ast, rightRef, null);
        }

        return ast;
    }

    protected JoinOperand parseTableReference(List<String> targetTables) throws ParseException {
        JoinOperand leftRef = parseTableFactor(targetTables);
        JoinConditionNode rightRef = parseJoinTail(targetTables);

        if (rightRef != null) {
            return new JoinConditionNode(rightRef.joinType(), leftRef, rightRef.right(), rightRef.conditions());
        }

        return leftRef;
    }

    protected JoinConditionNode parseJoinTail(List<String> targetTables) throws ParseException {
        if (match(TokenType.CROSS)) {
            consume(TokenType.JOIN);
            JoinOperand rightRef = parseTableFactor(targetTables);
            return new JoinConditionNode("CROSS", null, rightRef, null);
        }

        if (match(TokenType.INNER)) {
            consume(TokenType.JOIN);
            JoinOperand rightRef = parseTableFactor(targetTables);
            consume(TokenType.ON);
            return new JoinConditionNode("INNER", null, rightRef, parseWhereExpression());
        }

        if (match(TokenType.LEFT)) {
            match(TokenType.OUTER);
            consume(TokenType.JOIN);
            JoinOperand rightRef = parseTableFactor(targetTables);
            consume(TokenType.ON);
            return new JoinConditionNode("LEFT", null, rightRef, parseWhereExpression());
        }

        if (match(TokenType.RIGHT)) {
            match(TokenType.OUTER);
            consume(TokenType.JOIN);
            JoinOperand rightRef = parseTableFactor(targetTables);
            consume(TokenType.ON);
            return new JoinConditionNode("RIGHT", null, rightRef, parseWhereExpression());
        }

        if (match(TokenType.FULL)) {
            match(TokenType.OUTER);
            consume(TokenType.JOIN);
            JoinOperand rightRef = parseTableFactor(targetTables);
            consume(TokenType.ON);
            return new JoinConditionNode("FULL", null, rightRef, parseWhereExpression());
        }

        if (match(TokenType.NATURAL)) {
            consume(TokenType.JOIN);
            JoinOperand rightRef = parseTableFactor(targetTables);
            return new JoinConditionNode("NATURAL", null, rightRef, null);
        }

        if (match(TokenType.JOIN)) {
            JoinOperand rightRef = parseTableFactor(targetTables);
            consume(TokenType.ON);
            return new JoinConditionNode("INNER", null, rightRef, parseWhereExpression());
        }

        return null;

    }

    protected JoinOperand parseTableFactor(List<String> targetTables) throws ParseException {
        if(match(TokenType.LPARENTHESIS)) {
            JoinOperand table = parseTableReference(targetTables);
            consume(TokenType.RPARENTHESIS);
            return table;
        }

        Token tableToken = consume(TokenType.IDENTIFIER);
        targetTables.add(tableToken.getValue());
        return new TableNode(tableToken.getValue());
    }

}
