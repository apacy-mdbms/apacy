package com.apacy.queryprocessor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.apacy.common.dto.Column;
import com.apacy.common.dto.ParsedQuery;
import com.apacy.common.dto.Schema;
import com.apacy.common.dto.ast.expression.ColumnFactor;
import com.apacy.common.dto.ast.expression.ExpressionNode;
import com.apacy.common.dto.ast.expression.FactorNode;
import com.apacy.common.dto.ast.expression.TermNode;
import com.apacy.common.dto.ast.join.JoinConditionNode;
import com.apacy.common.dto.ast.join.JoinOperand;
import com.apacy.common.dto.ast.join.TableNode;
import com.apacy.common.dto.ast.where.BinaryConditionNode;
import com.apacy.common.dto.ast.where.ComparisonConditionNode;
import com.apacy.common.dto.ast.where.LiteralConditionNode;
import com.apacy.common.dto.ast.where.UnaryConditionNode;
import com.apacy.common.dto.ast.where.WhereConditionNode;
import com.apacy.common.dto.plan.CartesianNode;
import com.apacy.common.dto.plan.FilterNode;
import com.apacy.common.dto.plan.JoinNode;
import com.apacy.common.dto.plan.LimitNode;
import com.apacy.common.dto.plan.ModifyNode;
import com.apacy.common.dto.plan.PlanNode;
import com.apacy.common.dto.plan.ProjectNode;
import com.apacy.common.dto.plan.ScanNode;
import com.apacy.common.dto.plan.SortNode;
import com.apacy.common.interfaces.IStorageManager;

/**
 * Binds identifiers in the ParsedQuery by resolving table and column references,
 * producing an AST with fully qualified and validated names.
 */
public class QueryBinder {

    private final IStorageManager storageManager;

    public QueryBinder(IStorageManager storageManager) {
        this.storageManager = storageManager;
    }

    public ParsedQuery bind(ParsedQuery query) {
        if (query.targetTables() == null || query.targetTables().isEmpty()) {
            return query;
        }

        Map<String, String> aliasMap = query.aliasMap();
        List<String> tables = query.targetTables();

        for (String tableName : tables) {
            // Jika tableName ada di aliasMap sebagai key (alias), ambil real table-nya.
            // Jika tidak, asumsikan itu nama tabel asli.
            String realTable = tableName;
            if (aliasMap != null && aliasMap.containsKey(tableName)) {
                realTable = aliasMap.get(tableName);
            }
            
            if (storageManager.getSchema(realTable) == null) {
                throw new IllegalArgumentException("Semantic Error: Table '" + realTable + "' does not exist.");
            }
        }

        // Bind SELECT Columns
        List<String> resolvedColumns = new ArrayList<>();
        if (query.targetColumns() != null) {
            for (String colRaw : query.targetColumns()) {
                if (colRaw.equals("*")) {
                    // FIX: Expand '*' to all columns from all target tables
                    for (String tableRef : tables) {
                        String realTable = tableRef;
                        if (aliasMap != null && aliasMap.containsKey(tableRef)) {
                            realTable = aliasMap.get(tableRef);
                        }

                        Schema schema = storageManager.getSchema(realTable);
                        if (schema != null) {
                            for (Column c : schema.columns()) {
                                // Add as fully qualified name: tableRef.columnName
                                resolvedColumns.add(tableRef + "." + c.name());
                            }
                        }
                    }
                    continue;
                }
                resolvedColumns.add(resolveColumnName(colRaw, tables, aliasMap));
            }
        }

        // Bind ORDER BY
        String resolvedOrderBy = null;
        if (query.orderByColumn() != null) {
            resolvedOrderBy = resolveColumnName(query.orderByColumn(), tables, aliasMap);
        }

        // Bind WHERE
        Object resolvedWhere = null;
        if (query.whereClause() != null && query.whereClause() instanceof WhereConditionNode) {
            resolvedWhere = bindWhereCondition((WhereConditionNode) query.whereClause(), tables, aliasMap);
        }

        // Bind JOIN Conditions
        Object resolvedJoin = null;
        if (query.joinConditions() != null && query.joinConditions() instanceof JoinOperand) {
            resolvedJoin = bindJoinTree((JoinOperand) query.joinConditions(), tables, aliasMap);
        }

        return new ParsedQuery(
            query.queryType(),
            bindPlanTree(query.planRoot(), tables, aliasMap),
            query.targetTables(),
            resolvedColumns,
            query.values(),
            resolvedJoin,
            resolvedWhere,
            resolvedOrderBy,
            query.isDescending(),
            query.isOptimized(),
            query.limit(),
            query.offset(),
            query.aliasMap()
        );
    }

    // Recursive Where Binding
    private WhereConditionNode bindWhereCondition(WhereConditionNode node, List<String> tables, Map<String, String> aliasMap) {        
        if (node == null) {
            return null;
        }

        if (node instanceof BinaryConditionNode bin) {
            return new BinaryConditionNode(
                bindWhereCondition(bin.left(), tables, aliasMap),
                bin.operator(),
                bindWhereCondition(bin.right(), tables, aliasMap)
            );
        } 
        else if (node instanceof UnaryConditionNode unary) {
            return new UnaryConditionNode(
                unary.operator(),
                bindWhereCondition(unary.operand(), tables, aliasMap)
            );
        } 
        else if (node instanceof ComparisonConditionNode comp) {
            return new ComparisonConditionNode(
                bindExpression(comp.leftOperand(), tables, aliasMap),
                comp.operator(),
                bindExpression(comp.rightOperand(), tables, aliasMap)
            );
        } 
        else if (node instanceof LiteralConditionNode lit) {
            return lit; 
        }
        
        throw new IllegalArgumentException("Unknown WhereNode type: " + node.getClass());
    }

    // Recursive Expressiong Binding
    private ExpressionNode bindExpression(ExpressionNode expr, List<String> tables, Map<String, String> aliasMap) {
        TermNode newTerm = bindTerm(expr.term(), tables, aliasMap);

        List<ExpressionNode.TermPair> newRemainder = new ArrayList<>();
        for (ExpressionNode.TermPair pair : expr.remainderTerms()) {
            newRemainder.add(new ExpressionNode.TermPair(
                pair.additiveOperator(), 
                bindTerm(pair.term(), tables, aliasMap)
            ));
        }

        return new ExpressionNode(newTerm, newRemainder);
    }

    private TermNode bindTerm(TermNode term, List<String> tables, Map<String, String> aliasMap) {
        FactorNode newFactor = bindFactor(term.factor(), tables, aliasMap);

        List<TermNode.FactorPair> newRemainder = new ArrayList<>();

        if (term.remainderFactors() != null) {
            for (TermNode.FactorPair pair : term.remainderFactors()) {
                newRemainder.add(new TermNode.FactorPair(
                    pair.multiplicativeOperator(), 
                    bindFactor(pair.factor(), tables, aliasMap)
                ));
            }
        }

        return new TermNode(newFactor, newRemainder);
    }

    private FactorNode bindFactor(FactorNode factor, List<String> tables, Map<String, String> aliasMap) {
        if (factor instanceof ColumnFactor col) {
            String resolvedName = resolveColumnName(col.columnName(), tables, aliasMap);
            return new ColumnFactor(resolvedName);
        } 
        else if (factor instanceof ExpressionNode expr) {
            return bindExpression(expr, tables, aliasMap);
        }
        
        return factor;
    }

    // Recursive Join Binding
    private JoinOperand bindJoinTree(JoinOperand node, List<String> tables, Map<String, String> aliasMap) {
        if (node instanceof TableNode) {
            return node; 
        } 
        else if (node instanceof JoinConditionNode join) {
            JoinOperand newLeft = bindJoinTree(join.left(), tables, aliasMap);
            JoinOperand newRight = bindJoinTree(join.right(), tables, aliasMap);
            
            WhereConditionNode newConditions = bindWhereCondition(join.conditions(), tables, aliasMap);

            return new JoinConditionNode(
                join.joinType(), 
                newLeft, 
                newRight, 
                newConditions
            );
        }
        throw new IllegalArgumentException("Unknown JoinOperand type");
    }

    private PlanNode bindPlanTree(PlanNode node, List<String> tables, Map<String, String> aliasMap) {
        if (node == null) return null;

        // 1. FILTER NODE
        if (node instanceof FilterNode n) {
            PlanNode boundChild = bindPlanTree(n.child(), tables, aliasMap);
            
            Object boundPred = null;
            if (n.predicate() instanceof WhereConditionNode ast) {
                boundPred = bindWhereCondition(ast, tables, aliasMap);
            } else {
                boundPred = n.predicate(); 
            }

            return new FilterNode(boundChild, boundPred);
        }

        // 2. PROJECT NODE
        else if (node instanceof ProjectNode n) {
            PlanNode boundChild = bindPlanTree(n.child(), tables, aliasMap);
            
            List<String> boundCols = new ArrayList<>();
            if (n.columns() != null) {
                for (String col : n.columns()) {
                    if (col.equals("*")) {
                        boundCols.addAll(getOutputColumns(boundChild));
                    } else {
                        boundCols.add(resolveColumnName(col, tables, aliasMap));
                    }
                }
            }
            return new ProjectNode(boundChild, boundCols);
        }

        // 3. JOIN NODE
        else if (node instanceof JoinNode n) {
            PlanNode boundLeft = bindPlanTree(n.left(), tables, aliasMap);
            PlanNode boundRight = bindPlanTree(n.right(), tables, aliasMap);

            Object boundCond = null;
            String finalJoinType = n.joinType();

            if ("NATURAL".equalsIgnoreCase(n.joinType())) {
                List<String> leftCols = getOutputColumns(boundLeft);
                List<String> rightCols = getOutputColumns(boundRight);
                
                WhereConditionNode naturalCondition = null;

                for (String lColQualified : leftCols) {
                    String lColName = getColumnNameOnly(lColQualified);
                    
                    for (String rColQualified : rightCols) {
                        String rColName = getColumnNameOnly(rColQualified);

                        if (lColName.equals(rColName)) {
                            ComparisonConditionNode eq = createEqualityCondition(lColQualified, rColQualified);
                            
                            if (naturalCondition == null) {
                                naturalCondition = eq;
                            } else {
                                naturalCondition = new BinaryConditionNode(naturalCondition, "AND", eq);
                            }
                        }
                    }
                }
                
                if (naturalCondition != null) {
                    boundCond = naturalCondition;
                    finalJoinType = "INNER";
                } else {
                    finalJoinType = "CROSS"; 
                }

            } else {
                if (n.joinCondition() instanceof WhereConditionNode ast) {
                    boundCond = bindWhereCondition(ast, tables, aliasMap);
                } else {
                    boundCond = n.joinCondition();
                }
            }

            return new JoinNode(boundLeft, boundRight, boundCond, finalJoinType);
        }

        // 4. CARTESIAN NODE
        else if (node instanceof CartesianNode n) {
            PlanNode boundLeft = bindPlanTree(n.left(), tables, aliasMap);
            PlanNode boundRight = bindPlanTree(n.right(), tables, aliasMap);
            
            return new CartesianNode(boundLeft, boundRight);
        }

        // 5. SORT NODE
        else if (node instanceof SortNode n) {
            PlanNode boundChild = bindPlanTree(n.child(), tables, aliasMap);
            
            String boundCol = resolveColumnName(n.sortColumn(), tables, aliasMap);
            
            return new SortNode(boundChild, boundCol, n.ascending());
        }

        // 6. LIMIT NODE
        else if (node instanceof LimitNode n) {
            PlanNode boundChild = bindPlanTree(n.child(), tables, aliasMap);
            return new LimitNode(boundChild, n.limit(), n.offset());
        }

        // 7. MODIFY NODE
        else if (node instanceof ModifyNode n) {
            PlanNode boundChild = (n.child() != null) 
                ? bindPlanTree(n.child(), tables, aliasMap) 
                : null;
            
            List<Object> boundValues = null;
            if (n.values() != null) {
                boundValues = new ArrayList<>();
                for (Object val : n.values()) {
                    if (val instanceof com.apacy.common.dto.ast.expression.ExpressionNode expr) {
                        boundValues.add(bindExpression(expr, tables, aliasMap));
                    } else {
                        boundValues.add(val);
                    }
                }
            }

            return new ModifyNode(
                n.operation(),
                boundChild,
                n.targetTable(),
                n.targetColumns(),
                boundValues
            );
        }

        // 8. SCAN NODE
        else if (node instanceof ScanNode scan) {
            // Jika ScanNode punya kondisi (hasil pushdown optimizer), bind kondisinya juga
            Object boundCondition = null;
            if (scan.condition() instanceof WhereConditionNode ast) {
                boundCondition = bindWhereCondition(ast, tables, aliasMap);
            } else {
                boundCondition = scan.condition();
            }
            
            // Return ScanNode baru dengan kondisi yang sudah di-bind (resolved column names)
            return new ScanNode(
                scan.tableName(), 
                scan.alias(), 
                scan.indexName(), 
                boundCondition
            );
        }

        // 9. DDL NODE / TCL NODE
        else {
            return node;
        }
    }

    
    // Helper Resolution Logic
    private String resolveColumnName(String rawCol, List<String> tables, Map<String, String> aliasMap) {
        if (rawCol.contains(".")) {
            String[] parts = rawCol.split("\\.");
            String prefix = parts[0];
            String colName = parts[1];

            String realTable = null;

            if (aliasMap != null && aliasMap.containsKey(prefix)) {
                realTable = aliasMap.get(prefix);
            }
            else if (tables.contains(prefix)) {
                realTable = prefix;
            }
            
            if (realTable == null) {
                throw new IllegalArgumentException("Unknown table/alias reference: " + prefix);
            }

            validateColumnInCatalog(realTable, colName);
            return realTable + "." + colName;
        }

        String foundInTable = null;
        for (String table : tables) {
            // Check if 'table' is an alias
            String realTable = table;
            if (aliasMap != null && aliasMap.containsKey(table)) {
                realTable = aliasMap.get(table);
            }

            if (hasColumn(realTable, rawCol)) {
                if (foundInTable != null) {
                    throw new IllegalArgumentException("Ambiguous column: '" + rawCol + "' exists in " + foundInTable + " and " + table);
                }
                foundInTable = table; // Use the reference name (alias or table)
            }
        }

        if (foundInTable == null) {
            throw new IllegalArgumentException("Column '" + rawCol + "' not found in any target tables.");
        }
        return foundInTable + "." + rawCol;
    }

    private boolean hasColumn(String table, String col) {
        Schema s = storageManager.getSchema(table);
        return s != null && s.getColumnByName(col) != null;
    }

    private void validateColumnInCatalog(String table, String col) {
        if (!hasColumn(table, col)) {
            throw new IllegalArgumentException("Column '" + col + "' not found in table '" + table + "'");
        }
    }

    private List<String> getOutputColumns(PlanNode node) {
        if (node instanceof ScanNode scan) {
            Schema schema = storageManager.getSchema(scan.tableName());
            if (schema == null) return new ArrayList<>();
            String prefix = (scan.alias() != null && !scan.alias().isEmpty()) 
                            ? scan.alias() 
                            : scan.tableName();
            return schema.columns().stream()
                .map(col -> prefix + "." + col.name())
                .toList();
        } 
        else if (node instanceof ProjectNode proj) {
            return proj.columns();
        }
        else if (node instanceof FilterNode filter) {
            return getOutputColumns(filter.child());
        }
        else if (node instanceof SortNode sort) {
            return getOutputColumns(sort.child());
        } else if (node instanceof JoinNode join) {
             List<String> cols = new ArrayList<>(getOutputColumns(join.left()));
             cols.addAll(getOutputColumns(join.right()));
             return cols;
        }
        return new ArrayList<>(); 
    }

    private String getColumnNameOnly(String qualifiedName) {
        if (qualifiedName.contains(".")) {
            return qualifiedName.substring(qualifiedName.lastIndexOf('.') + 1);
        }
        return qualifiedName;
    }
    private ComparisonConditionNode createEqualityCondition(String leftCol, String rightCol) {
        FactorNode leftFactor = new ColumnFactor(leftCol);
        TermNode leftTerm = new TermNode(leftFactor, null);
        ExpressionNode leftExpr = new ExpressionNode(leftTerm, null);

        FactorNode rightFactor = new ColumnFactor(rightCol);
        TermNode rightTerm = new TermNode(rightFactor, null);
        ExpressionNode rightExpr = new ExpressionNode(rightTerm, null);

        return new ComparisonConditionNode(leftExpr, "=", rightExpr);
    }
}