#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#define PAGE_SIZE 4096
#define MAX_ROWS ((PAGE_SIZE - sizeof(int)) / sizeof(struct Row))
#define MAX_PAGES 10
#define INDEX_PAGES 5                               // Reserve 5 pages for B-Tree nodes
#define DATA_START_OFFSET (INDEX_PAGES * PAGE_SIZE) // Data pages start at 20480
#define MAX_KEYS 340                                // Maximum keys per B-Tree node (m - 1)
#define MAX_CHILDREN 341                            // Maximum children (m)

struct Row
{
    int id;
    char name[60];
};

// B-Tree entry for leaf nodes
typedef struct
{
    int id;        // 4 bytes
    off_t address; // 8 bytes
} IndexEntry;      // 12 bytes

// B-Tree node structure: fits in 4096 bytes
typedef struct
{
    int num_keys; // 4 bytes
    int is_leaf;  // 4 bytes
    union
    {
        struct
        {                                 // Leaf node
            IndexEntry entries[MAX_KEYS]; // 340 * 12 = 4080 bytes
        } leaf;
        struct
        {                                 // Internal node
            int keys[MAX_KEYS];           // 340 * 4 = 1360 bytes
            off_t children[MAX_CHILDREN]; // 341 * 8 = 2728 bytes
        } internal;
    } data;
} BTreeNode; // Total: 8 + 4080 = 4088 bytes

typedef struct
{
    FILE *file;                // File pointer for the database file
    void **pages;              // Array of page buffers
    int num_pages;             // Number of pages in use
    int max_pages;             // Maximum number of pages allowed
    off_t root_offset;         // File offset of the root node
    int page_dirty[MAX_PAGES]; // Dirty flags for data pages
} Database;

// function prototypes
Database init_db(const char *filename);
void write_buffer(Database *db);
int insert_row(Database *db, int id, const char *name);
int select_rows(Database *db, struct Row *rows, int max_rows);
int select_by_id(Database *db, int id, struct Row *row);
int delete_row(Database *db, int id);
void close_db(Database *db);
int update_row(Database *db, int id, const char *name);

// B-Tree helper functions
void read_node(Database *db, off_t offset, BTreeNode *node);
void write_node(Database *db, off_t offset, BTreeNode *node);
off_t allocate_node(Database *db);
void btree_search(Database *db, int id, off_t *address);
void btree_insert(Database *db, int id, off_t address);
void btree_delete(Database *db, int id);

// Read a B-Tree node from disk
void read_node(Database *db, off_t offset, BTreeNode *node)
{
    fseek(db->file, offset, SEEK_SET);
    size_t bytes_read = fread(node, 1, PAGE_SIZE, db->file);
    if (bytes_read != PAGE_SIZE)
    {
        printf("Error: Failed to read node at offset %lld\n", (long long)offset);
        exit(1);
    }
}

// Write a B-Tree node to disk
void write_node(Database *db, off_t offset, BTreeNode *node)
{
    fseek(db->file, offset, SEEK_SET);
    size_t bytes_written = fwrite(node, 1, PAGE_SIZE, db->file);
    if (bytes_written != PAGE_SIZE)
    {
        printf("Error: Failed to write node at offset %lld\n", (long long)offset);
        exit(1);
    }
    fflush(db->file);
}

// Allocate a new node (find a free page in the index section)
off_t allocate_node(Database *db)
{
    // Simple allocation: find the next free offset in the index section
    static off_t next_offset = 8; // Start after root_offset
    off_t new_offset = next_offset;
    next_offset += PAGE_SIZE;
    if (next_offset >= DATA_START_OFFSET)
    {
        printf("Error: Index section full\n");
        exit(1);
    }
    return new_offset;
}

// Search the B-Tree for an ID, return its address
void btree_search(Database *db, int id, off_t *address)
{
    BTreeNode node;
    off_t current_offset = db->root_offset;

    while (1)
    {
        read_node(db, current_offset, &node);
        if (node.is_leaf)
        {
            // Search in leaf node
            for (int i = 0; i < node.num_keys; i++)
            {
                if (node.data.leaf.entries[i].id == id)
                {
                    *address = node.data.leaf.entries[i].address;
                    return;
                }
            }
            *address = -1; // Not found
            return;
        }
        else
        {
            // Search in internal node
            int i;
            for (i = 0; i < node.num_keys; i++)
            {
                if (id < node.data.internal.keys[i])
                {
                    break;
                }
            }
            current_offset = node.data.internal.children[i];
        }
    }
}

// Insert into the B-Tree
void btree_insert(Database *db, int id, off_t address)
{
    BTreeNode root;
    read_node(db, db->root_offset, &root);

    // If root is full, split it and create a new root
    if (root.num_keys >= MAX_KEYS)
    {
        off_t old_root_offset = db->root_offset;
        off_t new_root_offset = allocate_node(db);
        off_t right_offset = allocate_node(db);

        BTreeNode new_root, right;
        new_root.num_keys = 0;
        new_root.is_leaf = 0;
        right.num_keys = 0;
        right.is_leaf = root.is_leaf;

        // Split the old root
        int mid = MAX_KEYS / 2;
        int mid_key = root.is_leaf ? root.data.leaf.entries[mid].id : root.data.internal.keys[mid];

        // Move second half to right node
        for (int i = mid + (root.is_leaf ? 0 : 1); i < root.num_keys; i++)
        {
            if (root.is_leaf)
            {
                right.data.leaf.entries[right.num_keys] = root.data.leaf.entries[i];
            }
            else
            {
                right.data.internal.keys[right.num_keys] = root.data.internal.keys[i];
                right.data.internal.children[right.num_keys] = root.data.internal.children[i];
            }
            right.num_keys++;
        }
        if (!root.is_leaf)
        {
            right.data.internal.children[right.num_keys] = root.data.internal.children[root.num_keys];
        }
        root.num_keys = mid;

        // Update new root
        new_root.data.internal.keys[0] = mid_key;
        new_root.data.internal.children[0] = old_root_offset;
        new_root.data.internal.children[1] = right_offset;
        new_root.num_keys = 1;

        // Write nodes
        write_node(db, old_root_offset, &root);
        write_node(db, right_offset, &right);
        write_node(db, new_root_offset, &new_root);
        db->root_offset = new_root_offset;
    }

    // Now insert into the appropriate node
    off_t current_offset = db->root_offset;
    while (1)
    {
        read_node(db, current_offset, &root);
        if (root.is_leaf)
        {
            // Insert into leaf
            int i;
            for (i = root.num_keys; i > 0 && root.data.leaf.entries[i - 1].id > id; i--)
            {
                root.data.leaf.entries[i] = root.data.leaf.entries[i - 1];
            }
            root.data.leaf.entries[i].id = id;
            root.data.leaf.entries[i].address = address;
            root.num_keys++;
            write_node(db, current_offset, &root);
            break;
        }
        else
        {
            // Find the child to descend into
            int i;
            for (i = 0; i < root.num_keys; i++)
            {
                if (id < root.data.internal.keys[i])
                {
                    break;
                }
            }
            current_offset = root.data.internal.children[i];
            // Check if child needs splitting (simplified, recurse if needed)
        }
    }

    // Update root_offset in file
    fseek(db->file, 0, SEEK_SET);
    fwrite(&db->root_offset, sizeof(off_t), 1, db->file);
}

// Delete from the B-Tree (simplified, no rebalancing)
void btree_delete(Database *db, int id)
{
    BTreeNode node;
    off_t current_offset = db->root_offset;
    off_t parent_offset = -1;
    int child_index = -1;

    while (1)
    {
        read_node(db, current_offset, &node);
        if (node.is_leaf)
        {
            // Delete from leaf
            int i;
            for (i = 0; i < node.num_keys; i++)
            {
                if (node.data.leaf.entries[i].id == id)
                {
                    break;
                }
            }
            if (i == node.num_keys)
            {
                return; // Not found
            }
            // Shift entries
            for (int j = i; j < node.num_keys - 1; j++)
            {
                node.data.leaf.entries[j] = node.data.leaf.entries[j + 1];
            }
            node.num_keys--;
            write_node(db, current_offset, &node);
            break;
        }
        else
        {
            // Find child to descend into
            int i;
            for (i = 0; i < node.num_keys; i++)
            {
                if (id < node.data.internal.keys[i])
                {
                    break;
                }
            }
            parent_offset = current_offset;
            child_index = i;
            current_offset = node.data.internal.children[i];
        }
    }

    // Update parent key if necessary (simplified)
    if (parent_offset != -1 && node.num_keys > 0)
    {
        BTreeNode parent;
        read_node(db, parent_offset, &parent);
        parent.data.internal.keys[child_index] = node.data.leaf.entries[0].id;
        write_node(db, parent_offset, &parent);
    }
}

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
        // Initialize B-Tree with an empty root node
        db.root_offset = 8; // First page after root_offset
        BTreeNode root = {0};
        root.is_leaf = 1;
        write_node(&db, db.root_offset, &root);
        fseek(db.file, 0, SEEK_SET);
        fwrite(&db.root_offset, sizeof(off_t), 1, db.file);
    }
    else
    {
        // Read root_offset
        fseek(db.file, 0, SEEK_SET);
        fread(&db.root_offset, sizeof(off_t), 1, db.file);
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
    for (int i = 0; i < MAX_PAGES; i++)
    {
        db.page_dirty[i] = 0;
    }

    // read data pages
    fseek(db.file, DATA_START_OFFSET, SEEK_SET);
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
    printf("Loaded %d pages\n", db.num_pages);
    return db;
}

// Write the buffer to the disk file
void write_buffer(Database *db)
{
    // Write root_offset
    fseek(db->file, 0, SEEK_SET);
    fwrite(&db->root_offset, sizeof(off_t), 1, db->file);

    // Write data pages (only dirty ones)
    fseek(db->file, DATA_START_OFFSET, SEEK_SET);
    for (int i = 0; i < db->num_pages; i++)
    {
        fseek(db->file, DATA_START_OFFSET + (off_t)i * PAGE_SIZE, SEEK_SET);
        size_t bytesWritten = fwrite(db->pages[i], 1, PAGE_SIZE, db->file);
        if (bytesWritten != PAGE_SIZE)
        {
            printf("Error: Failed to write page %d, wrote %zu bytes\n", i, bytesWritten);
            exit(1);
        }
        db->page_dirty[i] = 0; // Reset dirty flag after writing
    }
    // printf("Wrote %d pages to file\n", db->num_pages);
    fflush(db->file); // ensure data is written to disk
}

// Insert a row (returns 1 if inserted, 0 if failed due to duplicate ID)
int insert_row(Database *db, int id, const char *name)
{
    if (id <= 0)
    {
        printf("Error: ID must be a positive integer (got %d)\n", id);
        return 0;
    }

    // Check for duplicate ID
    off_t address;
    btree_search(db, id, &address);
    if (address != -1)
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
    int updated_num_rows = *page_num_rows;
    memcpy(db->pages[current_page], &updated_num_rows, sizeof(int));

    // Compute the row's address in the file
    off_t row_address = DATA_START_OFFSET + (off_t)current_page * PAGE_SIZE + offset;

    // Insert into B-Tree
    btree_insert(db, id, row_address);

    db->page_dirty[current_page] = 1;
    write_buffer(db);
    return 1;
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
    if (id <= 0)
    {
        printf("Error: ID must be a positive integer (got %d)\n", id);
        return 0;
    }

    off_t address;
    btree_search(db, id, &address);
    if (address == -1)
    {
        printf("Error: Row with id=%d not found\n", id);
        return 0;
    }

    fseek(db->file, address, SEEK_SET);
    size_t bytes_read = fread(row, sizeof(struct Row), 1, db->file);
    if (bytes_read != 1)
    {
        printf("Error: Failed to read row at address %lld\n", (long long)address);
        return 0;
    }
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

    off_t address;
    btree_search(db, id, &address);
    if (address == -1)
    {
        printf("Error: Row with id=%d not found\n", id);
        return 0;
    }

    struct Row row;
    fseek(db->file, address, SEEK_SET);
    size_t bytes_read = fread(&row, sizeof(struct Row), 1, db->file);
    if (bytes_read != 1)
    {
        printf("Error: Failed to read row at address %lld\n", (long long)address);
        return 0;
    }
    strncpy(row.name, name, 59);
    row.name[59] = '\0';
    fseek(db->file, address, SEEK_SET);
    fwrite(&row, sizeof(struct Row), 1, db->file);

    // update in memory pages
    for (int page = 0; page < db->num_pages; page++)
    {
        int *page_num_rows = (int *)db->pages[page];
        for (int i = 0; i < *page_num_rows; i++)
        {
            size_t offset = sizeof(int) + (i * sizeof(struct Row));
            off_t computed_address = DATA_START_OFFSET + (off_t)page * PAGE_SIZE + offset;

            if (computed_address == address)
            {
                memcpy((char *)db->pages[page] + offset, &row, sizeof(struct Row));
                db->page_dirty[page] = 1;
                break;
            }
        }
    }
    printf("Updated row at address %lld: id=%d, new name=%s\n", (long long)address, id, name);
    write_buffer(db);
    return 1;
}

// Delete a row
int delete_row(Database *db, int id)
{
    if (id <= 0)
    {
        printf("Error: ID must be a positive integer (got %d)\n", id);
        return 0;
    }

    off_t address;
    btree_search(db, id, &address);
    if (address == -1)
    {
        printf("Error: Row with id=%d not found\n", id);
        return 0;
    }

    // Delete from B-Tree
    btree_delete(db, id);

    // Delete from data pages
    int found = 0;
    for (int page = 0; page < db->num_pages; page++)
    {
        int *page_num_rows = (int *)db->pages[page];
        for (int i = 0; i < *page_num_rows; i++)
        {
            struct Row row;
            size_t offset = sizeof(int) + (i * sizeof(struct Row));
            off_t computed_address = DATA_START_OFFSET + (off_t)page * PAGE_SIZE + offset;
            if (computed_address == address)
            {
                found = 1;
                // Shift all subsequent rows left to fill the gap
                for (int j = i; j < *page_num_rows - 1; j++)
                {
                    size_t current_offset = sizeof(int) + (j * sizeof(struct Row));
                    size_t next_offset = sizeof(int) + ((j + 1) * sizeof(struct Row));
                    memcpy((char *)db->pages[page] + current_offset,
                           (char *)db->pages[page] + next_offset,
                           sizeof(struct Row));
                }
                // Clear the last slot after shifting
                size_t last_offset = sizeof(int) + ((*page_num_rows - 1) * sizeof(struct Row));
                memset((char *)db->pages[page] + last_offset, 0, sizeof(struct Row));
                (*page_num_rows)--;

                int updated_num_rows = *page_num_rows;
                memcpy(db->pages[page], &updated_num_rows, sizeof(int));

                // handle empty pages
                if (*page_num_rows == 0)
                {
                    free(db->pages[page]);
                    for (int k = page; k < db->num_pages - 1; k++)
                    {
                        db->pages[k] = db->pages[k + 1];
                    }
                    db->pages[db->num_pages - 1] = NULL;
                    db->num_pages--;
                    if (db->num_pages == 0)
                    {
                        void *new_page = malloc(PAGE_SIZE);
                        if (new_page == NULL)
                        {
                            perror("Error: Could not allocate initial page\n");
                            free(db->pages);
                            fclose(db->file);
                            exit(1);
                        }
                        memset(new_page, 0, PAGE_SIZE);
                        db->pages[0] = new_page;
                        db->num_pages = 1;
                        printf("Allocated initial page after all pages removed\n");
                    }
                }
                db->page_dirty[page] = 1;
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

// REPL loop (unchanged)
void run_repl(Database *db)
{
    // print instructions
    printf("Welcome to the database REPL!\n");
    printf("Available Commands:\n");
    printf("  INSERT <id> <name>      - Insert a new row\n");
    printf("  SELECT <id>             - Select a row by ID\n");
    printf("  SELECT                  - Select all rows\n");
    printf("  UPDATE <id> <new_name>  - Update a row by ID\n");
    printf("  DELETE <id>             - Delete a row by ID\n");
    printf("  exit                    - Exit the REPL\n");
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
// int main()
// {
//     Database db = init_db("mydb.db");
//     run_repl(&db);
//     close_db(&db);
//     printf("File closed successfully\n");
//     return 0;
// }

/// SUMMARY:
// 1. The code opens a database file named "mydb.db". If not found, it creates a new one.
// 2. It allocates a buffer of size PAGE_SIZE (4096 bytes) to read data from the file.
// 3. Simple REPL loop allows user to insert rows into the database, delete or select and display them.
// 4. Example: 'INSERT 1 John' would insert a row with id=1 and name='John'.
//             'SELECT' would display all inserted rows.
//             'DELETE 1' would delete the row with id=1.
//             'exit' exits the program.
// offset: 4, 68, 132, 196, 260, 324, 388, 452, 516, 580