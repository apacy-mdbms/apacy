# Branch Summary: `refactor/query-processor/pipelined-execution`

**Base Commit:** `5c0cb3a` (feat/query-processor/execute-query)  

## Overview
This branch performs a major architectural refactoring of the Query Processor module. It transitions the execution engine from a monolithic, immediate-execution model to a **Pipelined Execution Model** (also known as the Volcano Iterator Model). 

Instead of executing the entire query plan recursively in one go and passing large lists of rows between steps, the new engine constructs a tree of `Operator` objects. Rows are pulled through the pipeline one by one (or in small batches) via `next()` calls, significantly improving memory efficiency and extensibility.

## Key Changes

### 1. Introduction of the `Operator` Interface
A new interface `com.apacy.queryprocessor.execution.Operator` has been defined to standardize how query steps are executed.
- **`open()`**: Initializes the operator (e.g., opens files, builds hash tables).
- **`next()`**: Returns the next available `Row` or `null` if finished.
- **`close()`**: Releases resources.

### 2. Implementation of Operators
The logic previously embedded in `PlanTranslator` and `QueryProcessor` has been extracted into dedicated Operator classes:

*   **Data Access**:
    *   `ScanOperator`: Reads raw data blocks from the Storage Manager.
    *   `ModifyOperator`: Handles INSERT, UPDATE, and DELETE operations.
    *   `DDLOperator`: Handles CREATE/DROP TABLE operations.
*   **Relational Algebra**:
    *   `FilterOperator`: Applies `WHERE` clause predicates.
    *   `ProjectOperator`: Handles `SELECT` column projection.
    *   `JoinOperator` (and variants): Implements Nested Loop and Hash Joins.
    *   `CartesianOperator`: Handles Cartesian products.
    *   `SortOperator`: Handles `ORDER BY` clauses.
    *   `LimitOperator`: Handles `LIMIT` clauses.
*   **Transaction Control**:
    *   `TCLOperator`: Handles BEGIN, COMMIT, and ROLLBACK.

### 3. Refactoring `PlanTranslator`
`PlanTranslator.java` was massive reduced (~700 lines removed).
*   **Old Behavior**: Directly executed logic and returned `List<Row>`.
*   **New Behavior**: Acts as a "Builder" or "Factory". It traverses the `PlanNode` tree and constructs the corresponding `Operator` tree.

### 4. Refactoring `QueryProcessor`
*   The `executeQuery` method now orchestrates the lifecycle of the Operator tree (`build` -> `open` -> loop `next` -> `close`) instead of calling a single execution function.

## File Statistics
*   **New Files**: 12 Operator classes created in `com.apacy.queryprocessor.execution`.
*   **Modified Files**: 
    *   `PlanTranslator.java`: Heavy deletions (logic moved to operators).
    *   `QueryProcessor.java`: logic updated to use Operator interface.

## Commit History
*   `1fa9e5f`: Refactor Query Processor to use Pipelined (Volcano) Execution Model
*   `505939c`: stash (Work in progress/cleanup)
*   `dec8a1d`: stash
