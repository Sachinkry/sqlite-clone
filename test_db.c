#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

// Include the database functions (in a real project, you'd use a header file)
#define PAGE_SIZE 4096
#define MAX_ROWS ((PAGE_SIZE - sizeof(int)) / sizeof(struct Row))
#define MAX_PAGES 10

struct Row
{
    int id;
    char name[60];
};

typedef struct
{
    FILE *file;
    void **pages;
    int num_pages;
    int max_pages;
    off_t root_offset;
    int page_dirty[MAX_PAGES];
} Database;

// Function prototypes
Database init_db(const char *filename);
void write_buffer(Database *db);
int insert_row(Database *db, int id, const char *name);
int select_rows(Database *db, struct Row *rows, int max_rows);
int select_by_id(Database *db, int id, struct Row *row);
int delete_row(Database *db, int id);
void close_db(Database *db);
int update_row(Database *db, int id, const char *name);

// Test logging with colors
#define GREEN "\033[32m"
#define RED "\033[31m"
#define PURPLE "\033[35m"
#define RESET "\033[0m"

int total_tests = 0;
int passed_tests = 0;

void log_test(int test_num, const char *message, int passed)
{
    total_tests++;
    if (passed)
    {
        passed_tests++;
        printf("Test %d %s[PASSED] %s%s\n", test_num, GREEN, message, RESET);
    }
    else
    {
        printf("Test %d %s[FAILED] %s%s\n", test_num, RED, message, RESET);
    }
}

// Test insert, select, delete
void test_insert_select_delete()
{
    remove("test.db");
    Database db = init_db("test.db");

    // Test 1: Empty database
    struct Row rows[MAX_ROWS * MAX_PAGES];
    int count = select_rows(&db, rows, MAX_ROWS * MAX_PAGES);
    log_test(1, "Empty database should have 0 rows", count == 0);

    // Test 2: Insert one row and select
    int inserted = insert_row(&db, 1, "Alice");
    count = select_rows(&db, rows, MAX_ROWS * MAX_PAGES);
    log_test(2, "Should have 1 row after insert", inserted == 1 && count == 1 && rows[0].id == 1 && strcmp(rows[0].name, "Alice") == 0);

    // Test 3: Insert rows to fill first page (63 rows)
    for (int i = 2; i <= MAX_ROWS; i++)
    {
        char name[60];
        snprintf(name, 60, "Name%d", i);
        inserted = insert_row(&db, i, name);
        if (!inserted)
        {
            log_test(3, "Should insert up to 63 rows", 0);
            close_db(&db);
            return;
        }
    }
    count = select_rows(&db, rows, MAX_ROWS * MAX_PAGES);
    log_test(3, "Should have 63 rows after filling first page", count == MAX_ROWS);

    // Test 4: Insert row to trigger new page
    inserted = insert_row(&db, 64, "NewPage");
    count = select_rows(&db, rows, MAX_ROWS * MAX_PAGES);
    log_test(4, "Should have 64 rows after new page", inserted == 1 && count == MAX_ROWS + 1 && rows[MAX_ROWS].id == 64 && strcmp(rows[MAX_ROWS].name, "NewPage") == 0);

    // Test 5: Delete a row and select
    int deleted = delete_row(&db, 1);
    count = select_rows(&db, rows, MAX_ROWS * MAX_PAGES);
    log_test(5, "Should have 63 rows after delete", deleted == 1 && count == MAX_ROWS);

    // Test 6: Persistence after restart
    close_db(&db);
    db = init_db("test.db");
    count = select_rows(&db, rows, MAX_ROWS * MAX_PAGES);
    log_test(6, "Should have 63 rows after restart", count == MAX_ROWS);

    close_db(&db);
    remove("test.db"); // Ensure clean state for next suite
}

// Test select by ID
void test_select_by_id()
{
    remove("test.db");
    Database db = init_db("test.db");

    // Test 7: Empty database
    struct Row row;
    int found = select_by_id(&db, 1, &row);
    log_test(7, "Should not find any row in empty database", found == 0);

    // Test 8: Insert rows and select by ID
    int inserted = insert_row(&db, 1, "Alice");
    inserted &= insert_row(&db, 2, "Bob");
    inserted &= insert_row(&db, 100, "Charlie");
    if (!inserted)
    {
        log_test(8, "Failed to insert rows for select by ID test", 0);
        close_db(&db);
        return;
    }

    found = select_by_id(&db, 1, &row);
    log_test(8, "Should find row with ID 1", found == 1 && row.id == 1 && strcmp(row.name, "Alice") == 0);

    found = select_by_id(&db, 100, &row);
    log_test(9, "Should find row with ID 100", found == 1 && row.id == 100 && strcmp(row.name, "Charlie") == 0);

    // Test 10: Select non-existent ID
    found = select_by_id(&db, 999, &row);
    log_test(10, "Should not find row with ID 999", found == 0);

    // Test 11: Select deleted row
    int deleted = delete_row(&db, 2);
    found = select_by_id(&db, 2, &row);
    log_test(11, "Should not find deleted row with ID 2", deleted == 1 && found == 0);

    // Test 12: Persistence after restart
    close_db(&db);
    db = init_db("test.db");
    found = select_by_id(&db, 1, &row);
    log_test(12, "Should find row with ID 1 after restart", found == 1 && row.id == 1);

    close_db(&db);
    remove("test.db"); // Ensure clean state for next suite
}

// Test unique ID enforcement
void test_unique_id_enforcement()
{
    remove("test.db");
    Database db = init_db("test.db");

    // Test 13: Insert first row
    int inserted = insert_row(&db, 1, "Alice");
    struct Row rows[MAX_ROWS * MAX_PAGES];
    int count = select_rows(&db, rows, MAX_ROWS * MAX_PAGES);
    log_test(13, "Should insert first row with ID 1", inserted == 1 && count == 1 && rows[0].id == 1 && strcmp(rows[0].name, "Alice") == 0);

    // Test 14: Insert duplicate ID
    inserted = insert_row(&db, 1, "Bob");
    count = select_rows(&db, rows, MAX_ROWS * MAX_PAGES);
    log_test(14, "Should not insert duplicate ID 1", inserted == 0 && count == 1 && rows[0].id == 1 && strcmp(rows[0].name, "Alice") == 0);

    // Test 15: Insert new ID after duplicate attempt
    inserted = insert_row(&db, 2, "Bob");
    count = select_rows(&db, rows, MAX_ROWS * MAX_PAGES);
    log_test(15, "Should insert new row with ID 2", inserted == 1 && count == 2 && rows[0].id == 1 && rows[1].id == 2 && strcmp(rows[1].name, "Bob") == 0);

    close_db(&db);
    remove("test.db"); // Ensure clean state for next suite
}

// Test invalid inputs (negative IDs, zero IDs, etc.)
void test_invalid_inputs()
{
    remove("test.db");
    Database db = init_db("test.db");

    // Test 16: Insert negative ID
    int inserted = insert_row(&db, -1, "Invalid");
    struct Row rows[MAX_ROWS * MAX_PAGES];
    int count = select_rows(&db, rows, MAX_ROWS * MAX_PAGES);
    log_test(16, "Should not insert negative ID -1", inserted == 0 && count == 0);

    // Test 17: Insert zero ID
    inserted = insert_row(&db, 0, "Invalid");
    count = select_rows(&db, rows, MAX_ROWS * MAX_PAGES);
    log_test(17, "Should not insert zero ID", inserted == 0 && count == 0);

    // Test 18: Insert valid row, then duplicate
    inserted = insert_row(&db, 1, "Alice");
    count = select_rows(&db, rows, MAX_ROWS * MAX_PAGES);
    log_test(18, "Should insert first row with ID 1", inserted == 1 && count == 1 && rows[0].id == 1 && strcmp(rows[0].name, "Alice") == 0);

    inserted = insert_row(&db, 1, "Duplicate");
    count = select_rows(&db, rows, MAX_ROWS * MAX_PAGES);
    log_test(19, "Should not insert duplicate ID 1", inserted == 0 && count == 1 && rows[0].id == 1 && strcmp(rows[0].name, "Alice") == 0);

    // Test 20: Fill all pages to test max capacity (629 rows to reach 630 total)
    int successful_inserts = 0;
    for (int i = 2; i <= MAX_ROWS * MAX_PAGES; i++)
    { // 2 to 630 = 629 rows
        char name[60];
        snprintf(name, 60, "Name%d", i);
        inserted = insert_row(&db, i, name);
        if (inserted)
        {
            successful_inserts++;
        }
        else
        {
            log_test(20, "Should insert up to max rows", 0);
            // Don't return; continue to Test 21
        }
    }
    count = select_rows(&db, rows, MAX_ROWS * MAX_PAGES);
    log_test(20, "Should insert up to max rows", count == MAX_ROWS * MAX_PAGES && successful_inserts == MAX_ROWS * MAX_PAGES - 1);

    // Test 21: Insert after max rows reached
    inserted = insert_row(&db, MAX_ROWS * MAX_PAGES + 1, "TooMany");
    count = select_rows(&db, rows, MAX_ROWS * MAX_PAGES);
    log_test(21, "Should not insert after max rows reached", inserted == 0 && count == MAX_ROWS * MAX_PAGES);

    close_db(&db);
    remove("test.db"); // Ensure clean state for future runs
}

// Test update functionality
void test_update()
{
    remove("test.db");
    Database db = init_db("test.db");

    // Test 22: Insert a row to update
    int inserted = insert_row(&db, 1, "Alice");
    struct Row rows[MAX_ROWS * MAX_PAGES];
    int count = select_rows(&db, rows, MAX_ROWS * MAX_PAGES);
    log_test(22, "Should insert row with ID 1", inserted == 1 && count == 1 && rows[0].id == 1 && strcmp(rows[0].name, "Alice") == 0);

    // Test 23: Update existing row
    int updated = update_row(&db, 1, "Bob");
    count = select_rows(&db, rows, MAX_ROWS * MAX_PAGES);
    log_test(23, "Should update row with ID 1 to Bob", updated == 1 && count == 1 && rows[0].id == 1 && strcmp(rows[0].name, "Bob") == 0);

    // Test 24: Update non-existent row
    updated = update_row(&db, 2, "Charlie");
    count = select_rows(&db, rows, MAX_ROWS * MAX_PAGES);
    log_test(24, "Should not update non-existent row with ID 2", updated == 0 && count == 1 && rows[0].id == 1 && strcmp(rows[0].name, "Bob") == 0);

    // Test 25: Update with invalid ID
    updated = update_row(&db, -1, "Invalid");
    count = select_rows(&db, rows, MAX_ROWS * MAX_PAGES);
    log_test(25, "Should not update with invalid ID -1", updated == 0 && count == 1 && rows[0].id == 1 && strcmp(rows[0].name, "Bob") == 0);

    // Test 26: Persistence after restart
    close_db(&db);
    db = init_db("test.db");
    count = select_rows(&db, rows, MAX_ROWS * MAX_PAGES);
    log_test(26, "Should retain updated row after restart", count == 1 && rows[0].id == 1 && strcmp(rows[0].name, "Bob") == 0);

    close_db(&db);
    remove("test.db"); // Ensure clean state for next suite
}

// Test compaction functionality
void test_compaction()
{
    remove("test.db");
    Database db = init_db("test.db");

    // Test 27: Insert multiple rows and delete middle one
    int inserted = insert_row(&db, 1, "Alice");
    inserted &= insert_row(&db, 2, "Bob");
    inserted &= insert_row(&db, 3, "Charlie");
    struct Row rows[MAX_ROWS * MAX_PAGES];
    int count = select_rows(&db, rows, MAX_ROWS * MAX_PAGES);
    log_test(27, "Should insert 3 rows", inserted == 1 && count == 3 && rows[0].id == 1 && rows[1].id == 2 && rows[2].id == 3);

    int deleted = delete_row(&db, 2);
    count = select_rows(&db, rows, MAX_ROWS * MAX_PAGES);
    log_test(28, "Should compact rows after deleting ID 2", deleted == 1 && count == 2 && rows[0].id == 1 && rows[1].id == 3 && strcmp(rows[0].name, "Alice") == 0 && strcmp(rows[1].name, "Charlie") == 0);

    // Test 29: Fill a page, delete all, verify page removal
    for (int i = 4; i <= MAX_ROWS + 3; i++)
    {
        char name[60];
        snprintf(name, 60, "Name%d", i);
        inserted = insert_row(&db, i, name);
        if (!inserted)
        {
            log_test(29, "Should insert up to 63 rows total", 0);
            close_db(&db);
            return;
        }
    }
    count = select_rows(&db, rows, MAX_ROWS * MAX_PAGES);
    printf("Debug: After inserting IDs 4 to 66, total rows = %d, num_pages = %d\n", count, db.num_pages);
    int page_0_rows = *(int *)db.pages[0];
    log_test(29, "Should have 65 rows total, 63 in page 0", count == 65 && page_0_rows == MAX_ROWS);

    for (int i = 4; i <= MAX_ROWS + 3; i++)
    {
        deleted = delete_row(&db, i);
        if (!deleted)
        {
            log_test(29, "Should delete all rows", 0);
            close_db(&db);
            return;
        }
    }
    count = select_rows(&db, rows, MAX_ROWS * MAX_PAGES);
    printf("Debug: After deleting IDs 4 to 66, total rows = %d, num_pages = %d\n", count, db.num_pages);
    log_test(29, "Should remove empty page and retain 2 rows", count == 2 && db.num_pages == 1);

    // Test 30: Insert after compaction
    inserted = insert_row(&db, 4, "David");
    count = select_rows(&db, rows, MAX_ROWS * MAX_PAGES);
    log_test(30, "Should insert new row after compaction", inserted == 1 && count == 3 && rows[2].id == 4 && strcmp(rows[2].name, "David") == 0);

    close_db(&db);
    remove("test.db"); // Ensure clean state for next suite
}

int main()
{
    total_tests = 0;
    passed_tests = 0;
    test_insert_select_delete();
    test_select_by_id();
    test_unique_id_enforcement();
    test_invalid_inputs();
    test_update();
    test_compaction();
    printf("%s%d/%d tests passed!%s\n", PURPLE, passed_tests, total_tests, RESET);
    return 0;
}