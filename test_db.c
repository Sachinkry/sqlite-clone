#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

// Include the database functions (in a real project, you'd use a header file)
#define PAGE_SIZE 4096
#define MAX_ROWS ((PAGE_SIZE - sizeof(int)) / sizeof(struct Row))

struct Row
{
    int id;
    char name[60];
};

typedef struct
{
    FILE *file;
    void *buffer;
    int num_rows;
} Database;

Database init_db(const char *filename);
void write_buffer(Database *db);
void insert_row(Database *db, int id, const char *name);
int select_rows(Database *db, struct Row *rows, int max_rows);
int delete_row(Database *db, int id);
void close_db(Database *db);

// Helper function to find a row by ID
int select_by_id(Database *db, int id, struct Row *row)
{
    struct Row rows[MAX_ROWS];
    int count = select_rows(db, rows, MAX_ROWS);
    for (int i = 0; i < count; i++)
    {
        if (rows[i].id == id)
        {
            *row = rows[i];
            return 1;
        }
    }
    return 0;
}

// Test insert, select, delete (previous tests)
void test_insert_select_delete()
{
    remove("test.db");
    Database db = init_db("test.db");

    // Test 1: Empty database
    struct Row rows[MAX_ROWS];
    int count = select_rows(&db, rows, MAX_ROWS);
    assert(count == 0 && "Empty database should have 0 rows");

    // Test 2: Insert one row and select
    insert_row(&db, 1, "Alice");
    count = select_rows(&db, rows, MAX_ROWS);
    assert(count == 1 && "Should have 1 row after insert");
    assert(rows[0].id == 1 && "Row ID should be 1");
    assert(strcmp(rows[0].name, "Alice") == 0 && "Row name should be Alice");

    // Test 3: Insert another row and select
    insert_row(&db, 2, "Bob");
    count = select_rows(&db, rows, MAX_ROWS);
    assert(count == 2 && "Should have 2 rows after second insert");
    assert(rows[0].id == 1 && "First row ID should be 1");
    assert(rows[1].id == 2 && "Second row ID should be 2");
    assert(strcmp(rows[1].name, "Bob") == 0 && "Second row name should be Bob");

    // Test 4: Delete a row and select
    int deleted = delete_row(&db, 1);
    assert(deleted == 1 && "Row with ID 1 should be deleted");
    count = select_rows(&db, rows, MAX_ROWS);
    assert(count == 1 && "Should have 1 row after delete");
    assert(rows[0].id == 2 && "Remaining row ID should be 2");
    assert(strcmp(rows[0].name, "Bob") == 0 && "Remaining row name should be Bob");

    // Test 5: Delete non-existent row
    deleted = delete_row(&db, 999);
    assert(deleted == 0 && "Deleting non-existent row should return 0");

    // Test 6: Persistence after restart
    close_db(&db);
    db = init_db("test.db");
    count = select_rows(&db, rows, MAX_ROWS);
    assert(count == 1 && "Should have 1 row after restart");
    assert(rows[0].id == 2 && "Row ID should be 2 after restart");

    close_db(&db);
}

// Test select by ID
void test_select_by_id()
{
    remove("test.db");
    Database db = init_db("test.db");

    // Test 1: Empty database
    struct Row row;
    int found = select_by_id(&db, 1, &row);
    assert(found == 0 && "Should not find any row in empty database");

    // Test 2: Insert rows and select by ID
    insert_row(&db, 1, "Alice");
    insert_row(&db, 2, "Bob");
    insert_row(&db, 100, "Charlie");

    found = select_by_id(&db, 1, &row);
    assert(found == 1 && "Should find row with ID 1");
    assert(row.id == 1 && "Row ID should be 1");
    assert(strcmp(row.name, "Alice") == 0 && "Row name should be Alice");

    found = select_by_id(&db, 100, &row);
    assert(found == 1 && "Should find row with ID 100");
    assert(row.id == 100 && "Row ID should be 100");
    assert(strcmp(row.name, "Charlie") == 0 && "Row name should be Charlie");

    // Test 3: Select non-existent ID
    found = select_by_id(&db, 999, &row);
    assert(found == 0 && "Should not find row with ID 999");

    // Test 4: Select deleted row
    delete_row(&db, 2);
    found = select_by_id(&db, 2, &row);
    assert(found == 0 && "Should not find deleted row with ID 2");

    // Test 5: Persistence after restart
    close_db(&db);
    db = init_db("test.db");
    found = select_by_id(&db, 1, &row);
    assert(found == 1 && "Should find row with ID 1 after restart");
    assert(row.id == 1 && "Row ID should be 1 after restart");

    close_db(&db);
}

int main()
{
    test_insert_select_delete();
    test_select_by_id();
    printf("All tests passed!\n");
    return 0;
}