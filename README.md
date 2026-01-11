# Simple Python RDBMS

A lightweight, pure-Python Relational Database Management System implemented from scratch. This project demonstrates core database concepts including storage engines, indexing strategies, SQL parsing, and Hash Joins.

It includes an interactive **REPL**, a programmatic **Python API**, and a **Flask Web Interface**.

## 📂 Project Structure

- **`rdbms_core.py`**: The storage engine. Handles table structures, data types, `row_id` management, indexing, and basic CRUD (Create, Read, Update, Delete).
- **`rdbms_enhanced.py`**: Extends the core with advanced features like `JOIN` operations (using Hash Join algorithm) and the Interactive REPL.
- **`web_app.py`**: A Flask web application demonstrating the database in a real-world context (User/Order management).
- **`examples.py`**: A CLI demonstration script showing how to use the library programmatically.

## 🚀 Getting Started

### Prerequisites
The core database uses only the Python Standard Library. To run the Web Interface, you need Flask.

```bash
pip install flask
```

### Running the Demo
To see the database in action via the CLI:
```bash
python examples.py
```

### Running the Web Interface
To launch the graphical dashboard:
```bash
python web_app.py
```
Then open your browser to `http://127.0.0.1:5000`.

---

## 💻 SQL Syntax Reference

This RDBMS supports a subset of SQL. Commands are case-insensitive.

### Data Types
- `INTEGER`
- `REAL` (Float)
- `TEXT`
- `BOOLEAN`

### Commands

**1. Create Table**
Supports Primary Keys, Unique, and Not Null constraints.
```sql
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT UNIQUE)
```

**2. Insert Data**
Values can be quoted strings or numbers.
```sql
INSERT INTO users VALUES (1, 'Alice Smith', 'alice@example.com')
```

**3. Select Data**
Supports standard select, filtering, and column selection.
```sql
SELECT * FROM users
SELECT name, email FROM users WHERE id > 5
```

**4. Joins**
Supports `INNER JOIN` using a Hash Join algorithm.
```sql
SELECT users.name, orders.amount 
FROM orders 
JOIN users ON orders.user_id = users.id
```

**5. Update Data**
Updates indexes automatically upon modification.
```sql
UPDATE users SET email = 'new@test.com' WHERE name = 'Alice'
```

**6. Delete Data**
Removes rows and cleans up associated index pointers.
```sql
DELETE FROM users WHERE id = 1
```

**7. Indexing**
Create custom indexes to speed up lookups (automatically used by the engine).
```sql
CREATE INDEX idx_name ON users (name)
```

---

## 🧠 Architecture & Design

### Storage Model
Unlike simple implementations that use Lists (where deletion shifts all indices), this RDBMS uses a **Dictionary-based storage** model:
- **Structure:** `Dict[row_id, List[values]]`
- **Benefit:** Deleting a row is O(1) and strictly stable. A row ID never changes, ensuring index pointers remain valid without expensive reshuffling.

### Indexing Strategy
Indexes are implemented as `Dict[Value, Set[row_ids]]`.
- **Primary Keys:** Enforce uniqueness and allow O(1) lookup.
- **Secondary Indexes:** Allow fast filtering.
- **Maintenance:** The `insert`, `update`, and `delete` methods automatically hook into the index manager to ensure indexes are never stale (dangling pointers).

### Join Algorithm
The `rdbms_enhanced.py` implements a **Hash Join**:
1.  **Build Phase:** It scans the right-side table and builds a hash map: `{ join_key: [rows] }`.
2.  **Probe Phase:** It scans the left-side table one by one.
3.  **Match:** It looks up the key in the hash map.
4.  **Complexity:** O(M + N) — significantly faster than the O(M * N) Nested Loop join often found in simple toy databases.

### Persistence
The database supports `save()` and `load()` methods using Python's `pickle` module to serialize the entire database state (schema, data, and indexes) to disk.

---

## ⚠️ Limitations
*   **Transactions:** No ACID compliance (Atomicity, Consistency, Isolation, Durability).
*   **Concurrency:** Not thread-safe.
*   **Memory:** The entire database resides in RAM.
*   **Parsing:** The Regex-based parser is strict; complex nested queries or subqueries are not supported."# RDBMS" 
