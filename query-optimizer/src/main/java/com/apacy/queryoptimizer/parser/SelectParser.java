package com.apacy.queryoptimizer.parser;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.apacy.common.dto.ParsedQuery;
import com.apacy.common.dto.ast.join.JoinConditionNode;
import com.apacy.common.dto.ast.join.JoinOperand;
import com.apacy.common.dto.ast.join.TableNode;
import com.apacy.common.dto.ast.where.WhereConditionNode;
import com.apacy.common.dto.plan.LimitNode;
import com.apacy.common.dto.plan.PlanNode;
import com.apacy.common.dto.plan.SortNode;

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
            String col = consume(TokenType.IDENTIFIER).getValue();
            while(match(TokenType.DOT)) {
                Token t = peek();
                if (match(TokenType.IDENTIFIER)) {
                    col += '.' + t.getValue();
                } else if (match(TokenType.STAR)) {
                    col += ".*";
                } else { consume(TokenType.IDENTIFIER);} //throw error
            }
            targetColumns.add(col);
            while (match(TokenType.COMMA)) {
                String nextCol = consume(TokenType.IDENTIFIER).getValue();
                while(match(TokenType.DOT)) {
                    Token t = peek();
                    if (match(TokenType.IDENTIFIER)) {
                        nextCol += '.' + t.getValue();
                    } else if (match(TokenType.STAR)) {
                        nextCol += ".*";
                    } else { consume(TokenType.IDENTIFIER);} //throw error
                }
                targetColumns.add(nextCol);
            }
        }

        consume(TokenType.FROM);

        List<String> targetTables = new ArrayList<>();
        Map<String, String> aliasMap = new HashMap<>();
        JoinOperand joinAst = parseTableReferenceList(targetTables, aliasMap);

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
            String nextCol = consume(TokenType.IDENTIFIER).getValue();
            while(match(TokenType.DOT)) {
                Token t = peek();
                if (match(TokenType.IDENTIFIER)) {
                    nextCol += '.' + t.getValue();
                } else {consume(TokenType.IDENTIFIER);} //throw error
            }
            orderBy = nextCol;
            if (match(TokenType.DESC)) {
                isDesc = true;
            } else if (match(TokenType.ASC)) {
                isDesc = false;
            }
        }

        // --- UPDATE LOGIC LIMIT & OFFSET ---
        Integer limitValue = null;
        if (match(TokenType.LIMIT)) {
            Token limitToken = consume(TokenType.NUMBER_LITERAL);
            limitValue = Integer.parseInt(limitToken.getValue());
        }

        Integer offsetValue = null;
        if (match(TokenType.OFFSET)) {
            Token offsetToken = consume(TokenType.NUMBER_LITERAL);
            offsetValue = Integer.parseInt(offsetToken.getValue());
        }


        Object joinConditions = joinAst;
        Object whereClause = where;

        PlanNode planRoot = generatePlanNode((JoinOperand)joinAst, where, targetColumns);

        if (orderBy != null) {
            planRoot = new SortNode(planRoot, orderBy, isDesc);
        }

        if (limitValue != null) {
            int finalOffset = (offsetValue != null) ? offsetValue : 0;
            planRoot = new LimitNode(planRoot, limitValue, finalOffset);
        }

        consume(TokenType.SEMICOLON);
        consume(TokenType.EOF);

        // Gunakan konstruktor BARU yang ada limit & offset
        return new ParsedQuery(
                "SELECT",
                planRoot,
                targetTables,
                targetColumns,
                (List<Object>) null,
                joinConditions,
                whereClause,
                orderBy,
                isDesc,
                false,
                limitValue,   // Pass limit
                offsetValue,   // Pass offset
                aliasMap
        );
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
                if (match(TokenType.ASC) || match(TokenType.DESC)) {
                    // consume optional ASC/DESC
                }
            }

            // --- UPDATE VALIDASI LIMIT & OFFSET ---
            if (match(TokenType.LIMIT)) {
                if (peek().getType() != TokenType.NUMBER_LITERAL) { position = savedPos; return false; }
                position++;
            }

            if (match(TokenType.OFFSET)) {
                if (peek().getType() != TokenType.NUMBER_LITERAL) { position = savedPos; return false; }
                position++;
            }

            if (peek().getType() == TokenType.SEMICOLON) position++;

            return true;
        } catch (Exception e) {
            return false;
        }
    }

    protected JoinOperand parseTableReferenceList(List<String> targetTables, Map<String, String> aliasMap) throws ParseException {
        JoinOperand ast = parseTableReference(targetTables, aliasMap);

        while(match(TokenType.COMMA)) {
            JoinOperand rightRef = parseTableReference(targetTables, aliasMap);
            ast = new JoinConditionNode("CROSS", ast, rightRef, null);
        }

        return ast;
    }

    protected JoinOperand parseTableReference(List<String> targetTables, Map<String, String> aliasMap) throws ParseException {
        JoinOperand leftRef = parseTableFactor(targetTables, aliasMap);

        List<JoinConditionNode> tails = parseJoinTail(targetTables, aliasMap);

        for (JoinConditionNode tail : tails) {
            leftRef = new JoinConditionNode(
                    tail.joinType(),
                    leftRef,
                    tail.right(),
                    tail.conditions()
            );
        }

        return leftRef;
    }

    protected List<JoinConditionNode> parseJoinTail(List<String> targetTables, Map<String, String> aliasMap) throws ParseException {
        List<JoinConditionNode> joins = new ArrayList<>();

        while (true) {
            if (match(TokenType.CROSS)) {
                consume(TokenType.JOIN);
                JoinOperand rightRef = parseTableFactor(targetTables, aliasMap);
                joins.add(new JoinConditionNode("CROSS", null, rightRef, null));
                continue;
            }

            if (match(TokenType.INNER)) {
                consume(TokenType.JOIN);
                JoinOperand rightRef = parseTableFactor(targetTables, aliasMap);
                consume(TokenType.ON);
                joins.add(new JoinConditionNode("INNER", null, rightRef, parseWhereExpression()));
                continue;
            }

            if (match(TokenType.LEFT)) {
                match(TokenType.OUTER);
                consume(TokenType.JOIN);
                JoinOperand rightRef = parseTableFactor(targetTables, aliasMap);
                consume(TokenType.ON);
                joins.add(new JoinConditionNode("LEFT", null, rightRef, parseWhereExpression()));
                continue;
            }

            if (match(TokenType.RIGHT)) {
                match(TokenType.OUTER);
                consume(TokenType.JOIN);
                JoinOperand rightRef = parseTableFactor(targetTables, aliasMap);
                consume(TokenType.ON);
                joins.add(new JoinConditionNode("RIGHT", null, rightRef, parseWhereExpression()));
                continue;
            }

            if (match(TokenType.FULL)) {
                match(TokenType.OUTER);
                consume(TokenType.JOIN);
                JoinOperand rightRef = parseTableFactor(targetTables, aliasMap);
                consume(TokenType.ON);
                joins.add(new JoinConditionNode("FULL", null, rightRef, parseWhereExpression()));
                continue;
            }

            if (match(TokenType.NATURAL)) {
                consume(TokenType.JOIN);
                JoinOperand rightRef = parseTableFactor(targetTables, aliasMap);
                joins.add(new JoinConditionNode("NATURAL", null, rightRef, null));
                continue;
            }

            if (match(TokenType.JOIN)) {
                JoinOperand rightRef = parseTableFactor(targetTables, aliasMap);
                consume(TokenType.ON);
                joins.add(new JoinConditionNode("INNER", null, rightRef, parseWhereExpression()));
                continue;
            }
            break;
        }

        return joins;

    }

    protected JoinOperand parseTableFactor(List<String> targetTables, Map<String, String> aliasMap) throws ParseException {
        if(match(TokenType.LPARENTHESIS)) {
            JoinOperand table = parseTableReference(targetTables, aliasMap);
            consume(TokenType.RPARENTHESIS);
            return table;
        }

        Token tableToken = consume(TokenType.IDENTIFIER);
        targetTables.add(tableToken.getValue());

        if(match(TokenType.AS)) {
            Token alias = consume(TokenType.IDENTIFIER);
            aliasMap.put(alias.getValue(), tableToken.getValue());
        }

        return new TableNode(tableToken.getValue());
    }

}
