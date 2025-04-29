#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#define PAGE_SIZE 4096
#define MAX_ROWS ((PAGE_SIZE - sizeof(int)) / sizeof(struct Row))

struct Row
{
    int id;
    char name[60];
};

typedef struct
{
    FILE *file;   // File pointer for the database file
    void *buffer; // Buffer to hold data read from the file
    int num_rows; // Number of rows currently in the database
} Database;

// Initialize the database
Database init_db(const char *filename)
{
    Database db;
    db.file = fopen(filename, "r+");
    if (db.file == NULL)
    {
        db.file = fopen(filename, "w+");
        if (db.file == NULL)
        {
            perror("Error: Could not create file\n");
            exit(1);
        }
        fclose(db.file);
        db.file = fopen(filename, "r+");
        if (db.file == NULL)
        {
            perror("Error: Could not reopen file\n");
            exit(1);
        }
    }
    printf("File opened successfully at %p\n", (void *)db.file);

    db.buffer = malloc(PAGE_SIZE);
    if (db.buffer == NULL)
    {
        perror("Error: Could not allocate memory\n");
        fclose(db.file);
        exit(1);
    }
    printf("Buffer allocated successfully at %p\n", (void *)db.buffer);
    fseek(db.file, 0, SEEK_SET);
    size_t bytesRead = fread(db.buffer, 1, PAGE_SIZE, db.file);
    if (bytesRead < PAGE_SIZE && !feof(db.file))
    {
        printf("Error: Partial read, only %zu bytes read\n", bytesRead);
        free(db.buffer);
        fclose(db.file);
        exit(1);
    }
    printf("Read %zu bytes from file\n", bytesRead);
    // Load num_rows from buffer, or initialize to 0 if new file
    if (bytesRead == 0)
    {
        memset(db.buffer, 0, PAGE_SIZE); // Initialize buffer to zeros
        db.num_rows = 0;
        memcpy(db.buffer, &db.num_rows, sizeof(int)); // Store num_rows at the beginning of the buffer
        printf("Buffer initialized with zeros\n");
    }
    else
    {
        memcpy(&db.num_rows, db.buffer, sizeof(int)); // Load num_rows from buffer
        printf("Loaded num_rows: %d\n", db.num_rows);
    }

    return db;
}

// Write the buffer to the file
void write_buffer(Database *db)
{
    fseek(db->file, 0, SEEK_SET);
    size_t bytesWritten = fwrite(db->buffer, 1, PAGE_SIZE, db->file);
    if (bytesWritten != PAGE_SIZE)
    {
        printf("Error: Wrote %zu bytes, expected %d bytes\n", bytesWritten, PAGE_SIZE);
        exit(1);
    }
    fflush(db->file); // ensure data is written to disk
}

// insert a row
void insert_row(Database *db, int id, const char *name)
{
    if (db->num_rows >= MAX_ROWS)
    {
        printf("Error: Maximum rows reached, cannot insert more rows\n");
        return;
    };

    struct Row new_row;
    new_row.id = id;
    strncpy(new_row.name, name, 59);
    new_row.name[59] = '\0'; // Ensure null-termination

    // store row at correct offset: 4 bytes for num_rows + (num_rows * sizeof(struct Row))
    size_t offset = sizeof(int) + (db->num_rows * sizeof(struct Row));
    memcpy((char *)db->buffer + offset, &new_row, sizeof(struct Row));
    printf("Inserted row at offset %zu: id=%d, name=%s\n", offset, new_row.id, new_row.name);

    // increment num_rows and save it back to the buffer
    db->num_rows++;
    memcpy(db->buffer, &db->num_rows, sizeof(int));

    write_buffer(db);
}

// select all rows, returns count of non-deleted rows
int select_rows(Database *db, struct Row *rows, int max_rows)
{
    int count = 0;
    for (int i = 0; i < db->num_rows && count < max_rows; i++)
    {
        // calculate offset
        size_t offset = sizeof(int) + (i * sizeof(struct Row));
        struct Row row;
        memcpy(&row, (char *)db->buffer + offset, sizeof(struct Row));
        if (row.id != 0)
        {
            rows[count++] = row;
        }
    }
    return count;
}

// Select a row by ID (returns 1 if found, 0 if not)
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

// delete a row
int delete_row(Database *db, int id)
{
    int found = 0;
    for (int i = 0; i < db->num_rows; i++)
    {
        struct Row row;
        size_t offset = sizeof(int) + (i * sizeof(struct Row));
        memcpy(&row, (char *)db->buffer + offset, sizeof(struct Row));

        if (row.id == id)
        {
            found = 1;
            // set the row to zero
            memset((char *)db->buffer + offset, 0, sizeof(struct Row));
            break;
        }
    }
    if (found)
        write_buffer(db);

    return found;
}

// cleanup function
void close_db(Database *db)
{
    free(db->buffer);
    fclose(db->file);
}

// REPL loop
void run_repl(Database *db)
{
    char input[100];
    while (1)
    {
        printf("db>");
        fflush(stdout);
        // Read part of REPL loop --------
        if (fgets(input, sizeof(input), stdin) == NULL)
            break;                       // EOF or error
        input[strcspn(input, "\n")] = 0; // Remove newline character

        // Evaluate & Print part of REPL loop --------
        if (strncmp(input, "INSERT", 6) == 0)
        {
            int id;
            char name[60];
            if (sscanf(input, "INSERT %d %59s", &id, name) != 2)
            {
                printf("Error: Invalid INSERT format. Use: INSERT <id> <name>\n");
                continue;
            }
            insert_row(db, id, name);
            printf("Inserted row: id=%d, name=%s\n", id, name);
        }
        else if (strncmp(input, "SELECT", 6) == 0)
        {
            int id;
            char trailing[100];
            if (sscanf(input, "SELECT %d %s", &id, trailing) == 2)
            {
                printf("Error: Invalid SELECT format. Use: SELECT <id> or SELECT\n");
                continue;
            }
            if (sscanf(input, "SELECT %d", &id) == 1)
            {
                if (id <= 0)
                {
                    printf("Error: ID must be positive\n");
                    continue;
                }
                struct Row row;
                if (select_by_id(db, id, &row))
                {
                    printf("Row: id=%d, name=%s\n", row.id, row.name);
                }
                else
                {
                    printf("Row with id=%d not found\n", id);
                }
            }
            else
            {
                struct Row rows[MAX_ROWS];
                int count = select_rows(db, rows, MAX_ROWS);
                if (count == 0)
                {
                    printf("No rows to display\n");
                }
                else
                {
                    for (int i = 0; i < count; i++)
                    {
                        printf("Row %d: id=%d, name=%s\n", i, rows[i].id, rows[i].name);
                    }
                }
            }
        }
        else if (strncmp(input, "DELETE", 6) == 0)
        {
            int id;
            if (sscanf(input, "DELETE %d", &id) != 1)
            {
                printf("Error: Invalid DELETE format. Use: DELETE <id>\n");
                continue;
            }
            if (!delete_row(db, id))
            {
                printf("Row with id=%d not found\n", id);
            }
            else
            {
                printf("Deleted row with id=%d\n", id);
            }
        }
        else if (strncmp(input, "exit", 4) == 0)
        {
            break; // Exit the loop
        }
        else
        {
            printf("You entered: %s\n", input);
        }
    }
}

//! Comment this out while testing: test_db.c
int main()
{
    Database db = init_db("mydb.db");
    run_repl(&db);
    close_db(&db);
    printf("File closed successfully\n");
    return 0;
}

/// SUMMARY:
// 1. The code opens a database file named "mydb.db". If not found, it creates a new one.
// 2. It allocates a buffer of size PAGE_SIZE (4096 bytes) to read data from the file.
// 3. Simple REPL loop allows user to insert rows into the database, delete or select and display them.
// 4. Example: 'INSERT 1 John' would insert a row with id=1 and name='John'.
//             'SELECT' would display all inserted rows.
//             'DELETE 1' would delete the row with id=1.
//             'exit' exits the program.