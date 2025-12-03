package com.apacy.queryoptimizer.parser;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import com.apacy.common.dto.ParsedQuery;
import com.apacy.common.dto.plan.ModifyNode;
import com.apacy.common.dto.plan.PlanNode;
import com.apacy.queryoptimizer.ast.join.TableNode;
import com.apacy.queryoptimizer.ast.where.WhereConditionNode;

/**
 * Parser for UPDATE query
 */
public class UpdateParser extends AbstractParser {

    public UpdateParser(List<Token> tokens) {
        super(tokens);
    }

    @Override
    public ParsedQuery parse() throws ParseException {
        consume(TokenType.UPDATE);

        Token targetTable = consume(TokenType.IDENTIFIER);

        consume(TokenType.SET);

        List<String> targetColumns = new ArrayList<String>();
        List<Object> writeValues = new ArrayList<Object>();

        do {
            targetColumns.add(consume(TokenType.IDENTIFIER).getValue());

            String operator = consume(TokenType.OPERATOR).getValue();
            if (!operator.equals("=")) {
                throw new ParseException("Expected = operator after column name", position);
            }

            writeValues.add(parseExpression());

            if (!match(TokenType.COMMA)) {
                break;
            }
        } while (true);


        WhereConditionNode where = null;
        if (match(TokenType.WHERE)) {
            where = parseWhereExpression();
        }

        consume(TokenType.SEMICOLON);
        consume(TokenType.EOF);

        Object whereClause = where;

        TableNode sourceTable = new TableNode(targetTable.getValue());
        PlanNode searchPlan = generatePlanNode(sourceTable, where, null);

        ModifyNode planRoot = new ModifyNode("UPDATE", searchPlan, targetTable.getValue(), targetColumns, writeValues);
        return new ParsedQuery(
            "UPDATE",
            planRoot,
            List.of(targetTable.getValue()),
            targetColumns,
            writeValues,
            null,
            whereClause,
            null,
            false,
            false);
    };

    @Override
    public boolean validate() {
        try {
            consume(TokenType.UPDATE);
            consume(TokenType.IDENTIFIER);
            consume(TokenType.SET);
            do {
                consume(TokenType.IDENTIFIER).getValue();

                String operator = consume(TokenType.OPERATOR).getValue();
                if (!operator.equals("=")) {
                    return false;
                }
                parseExpression();

                if (!match(TokenType.COMMA)) {
                    break;
                }
            } while (true);


            if (match(TokenType.WHERE)) {
                parseWhereExpression();
            }

            return true;

        } catch (Exception e) {

            return false;
        }
    }
}
