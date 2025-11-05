# APACY - Mini DBMS

A mini Database Management System (DBMS) built with Java 17 and Maven. This project implements core DBMS components including query processing, query optimization, storage management, concurrency control, and failure recovery.

## Project Structure

This is a multi-module Maven project with the following components:

```
apacy/
├── common/                      # Common/Integration module
│   └── src/
│       ├── main/java/
│       │   └── com/apacy/common/
│       │       └── DBMSComponent.java
│       └── test/java/
│
├── query-processor/             # Query Processor module
│   └── src/
│       ├── main/java/
│       │   └── com/apacy/queryprocessor/
│       │       └── QueryProcessor.java
│       └── test/java/
│
├── query-optimization/          # Query Optimization module
│   └── src/
│       ├── main/java/
│       │   └── com/apacy/queryoptimization/
│       │       └── QueryOptimizer.java
│       └── test/java/
│
├── storage-manager/             # Storage Manager module
│   └── src/
│       ├── main/java/
│       │   └── com/apacy/storagemanager/
│       │       └── StorageManager.java
│       └── test/java/
│
├── concurrency-control/         # Concurrency Control Manager module
│   └── src/
│       ├── main/java/
│       │   └── com/apacy/concurrencycontrol/
│       │       └── ConcurrencyControlManager.java
│       └── test/java/
│
└── failure-recovery/            # Failure Recovery module
    └── src/
        ├── main/java/
        │   └── com/apacy/failurerecovery/
        │       └── FailureRecoveryManager.java
        └── test/java/
```

## Components

### 1. Common Module
The integration module that provides shared components and utilities used across all other modules. Contains the base `DBMSComponent` class that all components extend.

### 2. Query Processor
Responsible for parsing and processing database queries. Handles SQL query interpretation and execution.

### 3. Query Optimization
Optimizes database queries for better performance. Analyzes query plans and applies optimization techniques.

### 4. Storage Manager
Manages data storage and retrieval operations. Handles persistent storage of database data.

### 5. Concurrency Control Manager
Handles concurrent database transactions. Implements locking mechanisms and ensures transaction isolation.

### 6. Failure Recovery Manager
Provides database recovery and fault tolerance. Implements checkpoint creation and database recovery mechanisms.

## Requirements

- Java 17 or higher
- Maven 3.6 or higher

## Building the Project

To build all modules:

```bash
mvn clean install
```

To build a specific module:

```bash
cd <module-name>
mvn clean install
```

## Running Tests

To run all tests:

```bash
mvn test
```

To run tests for a specific module:

```bash
cd <module-name>
mvn test
```

## Module Dependencies

All modules depend on the `common` module which provides shared functionality:

- `common` - No dependencies (base module)
- `query-processor` - depends on `common`
- `query-optimization` - depends on `common`
- `storage-manager` - depends on `common`
- `concurrency-control` - depends on `common`
- `failure-recovery` - depends on `common`

## Development

### Adding New Components

1. Create a new class in the appropriate module
2. Extend from `DBMSComponent` if creating a new component
3. Implement required abstract methods: `initialize()` and `shutdown()`
4. Add corresponding unit tests

### Code Style

- Follow standard Java naming conventions
- Use meaningful variable and method names
- Add Javadoc comments for public methods and classes
- Write unit tests for new functionality

## License

This project is part of an educational initiative for learning database management systems.
