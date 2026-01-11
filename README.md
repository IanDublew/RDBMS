# PyRDBMS: A Simple In-Memory Relational Database Management System

![Python](https://img.shields.io/badge/python-3.8%2B-blue)

## Overview

**PyRDBMS** is a lightweight, Python-based implementation of a simple Relational Database Management System (RDBMS). It supports basic table creation with data types, CRUD operations, primary keys, foreign keys (with integrity checks), basic indexing, and simple joins. The interface uses a SQL-like syntax and includes an interactive REPL mode for querying. Persistence is handled via pickle serialization for durability across sessions.

This project was built from scratch to demonstrate core database concepts, including transaction support (`BEGIN`/`COMMIT`/`ROLLBACK` for ACID-like behavior) and query optimization via indexes and hash-joins. It aligns with the challenge requirements: declaring tables, CRUD, indexing, primary/unique keys, joining, SQL interface, REPL, and a trivial web app for CRUD demonstration.

> **Note on Credits:** This implementation is original work, inspired by standard database textbooks and Python's built-in data structures (e.g., dicts for tables/indexes). No external code was borrowed beyond standard libraries and Flask for the web app.

## Features

*   **Table Declaration:** Create tables with data types (`INTEGER`, `TEXT`, `REAL`, `BOOLEAN`, `DATE`) and constraints (`PRIMARY KEY`, `NOT NULL`, `FOREIGN KEY`, `UNIQUE`).
*   **CRUD Operations:**
    *   `CREATE TABLE`/`INDEX`
    *   `INSERT INTO` (with FK/PK/UNIQUE validation)
    *   `SELECT` (with `WHERE` conditions: `=`, `>`, `<`, `LIKE`; projections; aggregates: `SUM`, `COUNT`, `AVG`, `MIN`, `MAX`; `GROUP BY`)
    *   `UPDATE` (with `WHERE`)
    *   `DELETE` (with `WHERE`, enforcing FK integrity)
*   **Indexing:** Hash-based indexes for faster equality lookups (e.g., on PK, UNIQUE, or manually indexed columns).
*   **Keys:**
    *   **Primary Key:** Uniqueness enforcement, auto-increment-like row IDs.
    *   **Unique Key:** Fully enforced constraints preventing duplicate values.
    *   **Foreign Key:** Referential integrity checks on `INSERT` and `DELETE`.
*   **Joining:** `INNER JOIN` with `ON` conditions (hash-join optimized).
*   **Transactions:** `BEGIN`, `COMMIT`, `ROLLBACK` for atomic operations via a Write-Ahead Undo Log.
*   **REPL Mode:** Interactive SQL console with result tables and execution timing.
*   **Persistence:** Save/load DB state to/from files using `pickle`.
*   **Web App Demo:** A trivial Flask-based fintech ledger app showcasing CRUD via dashboard metrics and an in-browser SQL terminal.

### Limitations
*   Basic parser (no subqueries or nested complex expressions).
*   In-memory storage focus (though persistent via file serialization).
*   Not thread-safe for high-concurrency production environments.

## Requirements

*   Python 3.8+
*   **Flask** (for the web app)

## Installation

1.  **Clone the repository:**
    ```bash
    git clone <repo-url>
    cd py-rdbms
    ```

2.  **Install Flask:**
    ```bash
    pip install flask
    ```

## Usage

### Running the REPL Demo (`examples.py`)
This script demonstrates schema creation, data seeding, integrity checks, transactions, and launches the interactive REPL.

```bash
python examples.py
```

*   It creates a fintech-themed DB (`customers`, `wallets`, `tx_log`).
*   Simulates a transfer with `ROLLBACK` to demonstrate ACID compliance.
*   Runs analytic queries (aggregates, joins).
*   Enters **REPL mode**: Type SQL queries (e.g., `SELECT * FROM customers`) or `exit` to quit.

**Example Output:**
```text
✓ Schema applied. Integrity constraints active.
...
SQL> SELECT type, COUNT(*), SUM(amount) FROM tx_log GROUP BY type

TYPE    | COUNT | TOTAL VOLUME
------------------------------
CREDIT  |   2   | $5200.00
DEBIT   |   1   | $5000.00
FEE     |   2   | $300.00

(Duration: 1.23ms)
```

### Running the Web App (`web_app.py`)
A trivial Flask app for fintech ledger management, showcasing the RDBMS backend.

```bash
python web_app.py
```

Access at **[http://127.0.0.1:5000/](http://127.0.0.1:5000/)**

*   **Dashboard:** Views metrics (e.g., total USD balance via `SELECT SUM`), recent transactions (via `JOIN`).
*   **SQL Terminal:** Execute arbitrary SQL (CRUD) in-browser. Results update the DB immediately.
*   **Data Governance:** Lists tables; inspect raw memory rows and index structures ("God Mode").
*   **Reset System:** Re-seeds the database to a fresh state.

> **Example:** In the SQL Terminal, run:
> `INSERT INTO users VALUES (105, 'New Client', 'TIER_1', 'CA')`

## Project Structure

*   **`rdbms_core.py`**: Core RDBMS engine (Table structures, Storage, CRUD, Transaction Manager, Indexing logic).
*   **`rdbms_enhanced.py`**: Extensions (Hash-Joins, Aggregate Pipeline, Query Optimizer, REPL class).
*   **`examples.py`**: CLI demo script with an interactive console.
*   **`web_app.py`**: Flask web application for visual demonstration.
*   **`ledger.db` / `core_banking.db`**: Generated DB files (pickle-serialized).
*   **`audit.log`**: Logs executed queries for audit trails.

## SQL Examples

**Creating a Table**
```sql
CREATE TABLE employees (
    id INTEGER PRIMARY KEY, 
    name TEXT NOT NULL, 
    salary REAL, 
    dept_id INTEGER, 
    email TEXT UNIQUE,
    FOREIGN KEY (dept_id) REFERENCES departments(d_id)
)
```

**Inserting Data**
```sql
INSERT INTO employees VALUES (1, 'Alice', 75000.0, 101, 'alice@company.com')
```

**Query with Join**
```sql
SELECT employees.name, departments.name 
FROM employees 
JOIN departments ON employees.dept_id = departments.d_id
```

**Transaction with Rollback**
```sql
BEGIN;
UPDATE wallets SET balance = balance - 100 WHERE w_id = 1;
INSERT INTO tx_log VALUES (6, 1, 100.0, 'DEBIT');
-- Something goes wrong
ROLLBACK;
```

## Contributing
Feel free to fork and submit PRs for improvements, such as advanced SQL parsing (nested queries) or expanded data types.