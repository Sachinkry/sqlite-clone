# smalldb: A Minimal Disk-Based Database

## Overview

smalldb is a lightweight, educational disk-based database implemented in C, designed to help beginners understand the core concepts of databases like SQLite. It mimics key features of disk-based databases, such as B-Tree indexing, persistent storage, and efficient disk I/O operations, while keeping the implementation simple for learning purposes.

This project focuses on how databases manage data on disk, optimize disk I/O, and maintain data integrity. It’s a great starting point for anyone interested in database internals, especially if you’re new to C programming or disk-based storage systems.

### Features

- Persistent Storage: Stores data in a file (mydb.db) with 4096-byte pages, similar to SQLite’s page-based storage.
- B-Tree Indexing: Uses a B-Tree to index rows by id, enabling efficient lookups (3 disk reads for SELECT by id).

### Basic Operations:

- `INSERT <id> <name>` : Inserts a row with a unique id and name.
- `SELECT` : Lists all rows.
- `SELECT <id>` : Retrieves a row by id.
- `UPDATE <id> <new_name>` : Updates the name of a row by id.
- `DELETE <id>` : Deletes a row by id.

### Disk I/O Optimization:

- Uses a page_dirty flag to write only modified data pages, reducing unnecessary disk writes.
- Achieves 3 reads for lookups and 3-4 writes for deletions, aligning with efficient disk-based database design.
- Testing Suite: Includes test_db.c with 30 test cases to verify functionality, covering insertion, selection, deletion, updates, and persistence.
- Simple REPL: Interactive command-line interface to execute database operations.

Project Structure

```
smalldb/
├── README.md
├── main.c
├── mydb.db
├── test_db.c
```

- db.c: Core database implementation, including B-Tree indexing, disk I/O, and operation logic.
- test_db.c: Test suite to verify the database’s functionality.
- mydb.db: The database file where data is stored (created automatically).
