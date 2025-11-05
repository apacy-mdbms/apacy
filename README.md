# mDBMS-Apacy

**Modular Database Management System - Super Group Apacy**

## Overview

mDBMS-Apacy is a comprehensive, modular database management system built with Java 17 and Maven. This project demonstrates the implementation of core DBMS components including storage management, query processing, concurrency control, and failure recovery.

## Architecture

The system is organized into 6 main modules:

### 1. Common (`common/`)
The "Kitab Suci" module containing shared DTOs, enums, and interfaces used across all other modules.

### 2. Storage Manager (`storage-manager/`)
Handles physical data storage, including:
- Block-level data management
- Serialization/deserialization
- Hash indexing (required) and B+ Tree (bonus)
- Statistics collection

### 3. Query Optimizer (`query-optimizer/`)
Responsible for query parsing and optimization:
- SQL query parsing
- Heuristic-based optimization
- Cost estimation using storage statistics

### 4. Query Processor (`query-processor/`)
The main execution engine that coordinates all other modules:
- CLI interface (Main entry point)
- Query execution strategies
- JOIN and sorting implementations

### 5. Concurrency Control (`concurrency-control/`)
Manages concurrent access to data:
- Lock-based concurrency control
- Timestamp-based concurrency control
- Deadlock detection and handling

### 6. Failure Recovery (`failure-recovery/`)
Ensures data consistency and recovery:
- Write-ahead logging
- UNDO/REDO recovery mechanisms
- Checkpoint management

## Building and Running

### Prerequisites
- Java 17 or higher
- Maven 3.6+

### Build
```bash
mvn clean compile
```

### Run
```bash
cd query-processor
mvn exec:java -Dexec.mainClass="com.apacy.queryprocessor.Main"
```

### Test
```bash
mvn test
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
