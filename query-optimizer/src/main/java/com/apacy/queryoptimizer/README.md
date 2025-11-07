# CFG Rules

CFG production rules untuk parser

```
SelectQuery = SELECT SelectList FROM TableList WhereClause OrderBy Limit;
UpdateQuery = UPDATE Identifier SET Condition FROM TableList WhereClause;
InsertQuery = INSERT INTO Identifier VALUES (UpdateValues);
DeleteQuery = DELETE FROM Identifier WhereClause;

SelectList       = Identifier | Identifier, SelectList | *
TableList        = Identifier
                     | Identifier, TableList
                     | Identifier NATURAL JOIN Identifier
                     | Identifier JOIN Identifier ON WhereCondition
WhereClause      = WHERE WhereCondition | epsilon
WhereCondition   = WhereCondition LogicalOperator WhereCondition
                     | Identifier EqualityOperator Literal
                     | UnaryOperator WhereCondition
                     | Literal
OrderBy          = ORDER BY Identifier ASC | ORDER BY Identifier DESC | epsilon
Limit            = LIMIT Number | epsilon
UpdateCondition  = UpdateTerm UpdateCondition | UpdateTerm
UpdateTerm       = Identifier Operator Literal
UpdateValues     = Identifier | Identifier, UpdateValues
Operator         = + | - | * | / | % | ^
EqualityOperator = = | > | < | >= | <= | <>
LogicalOperator  = AND | OR
UnaryOperator    = NOT
Literal          = String | Number
```