#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

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
    FILE *file;    // File pointer for the database file
    void **pages;  // Array of page buffers
    int num_pages; // Number of pages in use
    int max_pages; // Maximum number of pages allowed
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

    db.max_pages = MAX_PAGES;
    db.pages = malloc(db.max_pages * sizeof(void *)); // 10 * 8 bytes
    if (db.pages == NULL)
    {
        perror("Error: Could not allocate pages array\n");
        fclose(db.file);
        exit(1);
    }
    db.num_pages = 0;

    // read all pages
    fseek(db.file, 0, SEEK_SET);
    void *temp_buffer = malloc(PAGE_SIZE);
    if (temp_buffer == NULL)
    {
        perror("Error: Could not allocate temp buffer\n");
        free(db.pages);
        fclose(db.file);
        exit(1);
    }

    while (1)
    {
        size_t bytesRead = fread(temp_buffer, 1, PAGE_SIZE, db.file);
        if (bytesRead == 0)
            break;
        if (bytesRead < PAGE_SIZE && !feof(db.file))
        {
            printf("Error: Partial read, only %zu bytes read\n", bytesRead);
            free(temp_buffer);
            free(db.pages);
            fclose(db.file);
            exit(1);
        }
        printf("Read %zu bytes from file for page %d\n", bytesRead, db.num_pages);

        void *page = malloc(PAGE_SIZE);
        if (page == NULL)
        {
            perror("Error: Could not allocate page\n");
            free(temp_buffer);
            free(db.pages);
            fclose(db.file);
            exit(1);
        }
        memcpy(page, temp_buffer, PAGE_SIZE);
        db.pages[db.num_pages] = page;
        db.num_pages++;

        if (db.num_pages >= db.max_pages)
        {
            printf("Warning: Maximum pages reached\n");
            break;
        }
    }
    free(temp_buffer);

    if (db.num_pages == 0)
    {
        void *page = malloc(PAGE_SIZE);
        if (page == NULL)
        {
            perror("Error: Could not allocate first page\n");
            free(db.pages);
            fclose(db.file);
            exit(1);
        }
        memset(page, 0, PAGE_SIZE); // Initialize the first page to zero
        db.pages[0] = page;
        db.num_pages = 1;
        printf("Allocated first page\n");
    }
    else
    {
        printf("Loaded %d pages from file\n", db.num_pages);
    }

    return db;
}

// Write the buffer to the file
void write_buffer(Database *db)
{
    fseek(db->file, 0, SEEK_SET);
    for (int i = 0; i < db->num_pages; i++)
    {
        size_t bytesWritten = fwrite(db->pages[i], 1, PAGE_SIZE, db->file);
        if (bytesWritten != PAGE_SIZE)
        {
            printf("Error: Wrote %zu bytes for page %d, expected %d bytes\n", bytesWritten, i, PAGE_SIZE);
            break;
        }
    }
    fflush(db->file); // ensure data is written to disk
}

// select all rows, returns count of non-deleted rows
int select_rows(Database *db, struct Row *rows, int max_rows)
{
    int count = 0;
    for (int page = 0; page < db->num_pages && count < max_rows; page++)
    {
        int *page_num_rows = (int *)db->pages[page]; // Pointer to the number of rows in the page
        for (int i = 0; i < *page_num_rows && count < max_rows; i++)
        {
            size_t offset = sizeof(int) + (i * sizeof(struct Row));
            struct Row temp_row;
            memcpy(&temp_row, (char *)db->pages[page] + offset, sizeof(struct Row));
            if (temp_row.id != 0) // Check if row is not deleted
            {
                rows[count++] = temp_row;
            }
        }
    }
    return count;
}

// Select a row by ID (returns 1 if found, 0 if not)
int select_by_id(Database *db, int id, struct Row *row)
{
    for (int page = 0; page < db->num_pages; page++)
    {
        int *page_num_rows = (int *)db->pages[page]; // Pointer to the number of rows in the page
        for (int i = 0; i < *page_num_rows; i++)
        {
            size_t offset = sizeof(int) + (i * sizeof(struct Row));
            struct Row temp_row;
            memcpy(&temp_row, (char *)db->pages[page] + offset, sizeof(struct Row));
            if (temp_row.id == id)
            {
                *row = temp_row;
                return 1;
            }
        }
    }
    return 0;
}

// Insert a row (returns 1 if inserted, 0 if failed due to duplicate ID)
int insert_row(Database *db, int id, const char *name)
{
    if (id <= 0)
    {
        printf("Error: ID must be a positive integer (got %d)\n", id);
        return 0;
    }
    struct Row row;
    if (select_by_id(db, id, &row))
    {
        printf("Error: Row with id=%d already exists\n", id);
        return 0;
    }

    int current_page = db->num_pages - 1;
    int *page_num_rows = (int *)db->pages[current_page]; // Pointer to the number of rows in the page
    if (*page_num_rows >= MAX_ROWS)
    {
        if (db->num_pages >= db->max_pages)
        {
            printf("Error: Maximum pages reached, cannot insert more rows\n");
            return 0;
        }
        void *new_page = malloc(PAGE_SIZE);
        if (new_page == NULL)
        {
            printf("Error: Could not allocate new page\n");
            return 0;
        }
        memset(new_page, 0, PAGE_SIZE); // Initialize the new page to zero
        db->pages[db->num_pages] = new_page;
        db->num_pages++;
        current_page = db->num_pages - 1;
        page_num_rows = (int *)db->pages[current_page]; // Update pointer to the new page
        printf("Allocated new page %d\n", current_page);
    }

    struct Row new_row;
    new_row.id = id;
    strncpy(new_row.name, name, 59);
    new_row.name[59] = '\0';

    size_t offset = sizeof(int) + (*page_num_rows * sizeof(struct Row));
    memcpy((char *)db->pages[current_page] + offset, &new_row, sizeof(struct Row));
    printf("Inserted row at offset %zu in page %d: id=%d, name=%s\n", offset, current_page, new_row.id, new_row.name);

    (*page_num_rows)++;
    memcpy(db->pages[current_page], page_num_rows, sizeof(int));
    write_buffer(db);
    return 1;
}

// update a row
int update_row(Database *db, int id, const char *name)
{
    if (id <= 0)
    {
        printf("Error: ID must be a positive integer (got %d)\n", id);
        return 0;
    }
    struct Row row;
    if (!select_by_id(db, id, &row))
    {
        printf("Error: Row with id=%d not found\n", id);
        return 0;
    }

    strncpy(row.name, name, 59);
    row.name[59] = '\0';

    for (int page = 0; page < db->num_pages; page++)
    {
        int *page_num_rows = (int *)db->pages[page];
        for (int i = 0; i < *page_num_rows; i++)
        {
            size_t offset = sizeof(int) + (i * sizeof(struct Row));
            struct Row temp_row;
            memcpy(&temp_row, (char *)db->pages[page] + offset, sizeof(struct Row));
            if (temp_row.id == id)
            {
                memcpy((char *)db->pages[page] + offset, &row, sizeof(struct Row));
                write_buffer(db);
                return 1;
            }
        }
    }
    return 0;
}

// delete a row
int delete_row(Database *db, int id)
{
    int found = 0;
    for (int page = 0; page < db->num_pages; page++)
    {
        int *page_num_rows = (int *)db->pages[page];
        for (int i = 0; i < *page_num_rows; i++)
        {
            struct Row row;
            size_t offset = sizeof(int) + (i * sizeof(struct Row));
            memcpy(&row, (char *)db->pages[page] + offset, sizeof(struct Row));
            if (row.id == id)
            {
                found = 1;
                memset((char *)db->pages[page] + offset, 0, sizeof(struct Row));
                break;
            }
        }
        if (found)
            break;
    }
    if (found)
        write_buffer(db);

    return found;
}

// cleanup function
void close_db(Database *db)
{
    for (int i = 0; i < db->num_pages; i++)
    {
        free(db->pages[i]);
    }
    free(db->pages);
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
            struct Row row;
            if (sscanf(input, "INSERT %d %59s", &id, name) != 2)
            {
                printf("Error: Invalid INSERT format. Use: INSERT <id> <name>\n");
                continue;
            }
            if (id <= 0)
            {
                printf("Error: ID must be a positive integer (got %d)\n", id);
                continue;
            }

            if (insert_row(db, id, name))
            {
                printf("Inserted row: id=%d, name=%s\n", id, name);
            }
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
                    printf("Error: ID must be a positive integer (got %d)\n", id);
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
        else if (strncmp(input, "UPDATE", 6) == 0)
        {
            int id;
            char name[60];
            if (sscanf(input, "UPDATE %d %59s", &id, name) != 2)
            {
                printf("Error: Invalid UPDATE format. Use: UPDATE <id> <new_name>\n");
                continue;
            }
            if (id <= 0)
            {
                printf("Error: ID must be a positive integer (got %d)\n", id);
                continue;
            }
            if (update_row(db, id, name))
            {
                printf("Updated row: id=%d, new name=%s\n", id, name);
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
            if (id <= 0)
            {
                printf("Error: ID must be a positive integer (got %d)\n", id);
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