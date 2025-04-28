#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define PAGE_SIZE 4096
#define MAX_ROWS (PAGE_SIZE - sizeof(int) / sizeof(struct Row))

struct Row
{
    int id;
    char name[60];
};

int main()
{
    FILE *file = fopen("mydb.db", "r+");
    if (file == NULL)
    {
        file = fopen("mydb.db", "w+");
        if (file == NULL)
        {
            perror("Error: Could not create file\n");
            return 1;
        }
        fclose(file);
        file = fopen("mydb.db", "r+");
        if (file == NULL)
        {
            perror("Error: Could not reopen file\n");
            return 1;
        }
    }
    printf("File opened successfully at %p\n", (void *)file);

    // Allocate a buffer for a page
    void *buffer = malloc(PAGE_SIZE);

    if (buffer == NULL)
    {
        perror("Error: Could not allocate memory\n");
        fclose(file);
        return 1;
    }
    printf("Buffer allocated successfully at %p\n", (void *)buffer);

    fseek(file, 0, SEEK_SET);
    size_t bytesRead = fread(buffer, 1, PAGE_SIZE, file);
    if (bytesRead < PAGE_SIZE && !feof(file))
    {
        printf("Error: Partial read, only %zu bytes read\n", bytesRead);
        free(buffer);
        fclose(file);
        return 1;
    }
    printf("Read %zu bytes from file\n", bytesRead);

    // Load num_rows from buffer, or initialize to 0 if new file
    int num_rows;
    if (bytesRead == 0)
    {
        memset(buffer, 0, PAGE_SIZE);
        num_rows = 0;
        memcpy(buffer, &num_rows, sizeof(int));
        printf("Buffer initialized with zeros\n");
    }
    else
    {
        memcpy(&num_rows, buffer, sizeof(int)); // Load num_rows from buffer
        printf("Loaded num_rows: %d\n", num_rows);
    }

    // Simple REPL loop: Read-Eval-Print Loop
    char input[100];
    while (1)
    {
        printf("db>");
        fflush(stdout);
        if (fgets(input, sizeof(input), stdin) == NULL)
        {
            break; // EOF or error
        }

        input[strcspn(input, "\n")] = 0; // Remove newline character

        if (strncmp(input, "INSERT", 6) == 0)
        {
            if (num_rows >= MAX_ROWS)
            {
                printf("Error: Maximum rows reached, cannot insert more rows\n");
                continue;
            };

            struct Row new_row;
            int id;
            char name[60];

            if (sscanf(input, "INSERT %d %59s", &id, name) == 2)
            {
                new_row.id = id;
                strncpy(new_row.name, name, 59);
                new_row.name[59] = '\0'; // Ensure null-termination

                // store row at correct offset: 4 bytes for num_rows + (num_rows * sizeof(struct Row))
                size_t offset = sizeof(int) + (num_rows * sizeof(struct Row));
                memcpy((char *)buffer + offset, &new_row, sizeof(struct Row));
                printf("Inserted row at offset %zu: id=%d, name=%s\n", offset, new_row.id, new_row.name);

                // increment num_rows and save it back to the buffer
                num_rows++;
                memcpy(buffer, &num_rows, sizeof(int));

                // Write the buffer back to the file
                fseek(file, 0, SEEK_SET);
                size_t bytesWritten = fwrite(buffer, 1, PAGE_SIZE, file);
                if (bytesWritten != PAGE_SIZE)
                {
                    printf("Error: Wrote %zu bytes, expected %d bytes\n", bytesWritten, PAGE_SIZE);
                }
                else
                {
                    fflush(file); // ensure data is written to disk
                    printf("Buffer written to file successfully\n");
                }
            }
            else
            {
                printf("Error: Invalid INSERT format. Use: INSERT <id> <name>\n");
            }
        }
        else if (strncmp(input, "SELECT", 6) == 0)
        {
            if (num_rows == 0)
            {
                printf("No rows to display\n");
            }
            else
            {
                for (int i = 0; i < num_rows; i++)
                {
                    struct Row row;
                    size_t offset = sizeof(int) + (i * sizeof(struct Row));
                    memcpy(&row, (char *)buffer + offset, sizeof(struct Row));
                    printf("Row %d (offset %zu): id=%d, name=%s\n", i, offset, row.id, row.name);
                }
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

    // Cleanup
    free(buffer);
    fclose(file);
    printf("File closed successfully\n");
    return 0;
}

// ?SUMMARY:
// 1. The code opens a database file named "mydb.db". If not found, it creates a new one.
// 2. It allocates a buffer of size PAGE_SIZE (4096 bytes) to read data from the file.
// 3. Simple REPL loop allows user to insert rows into the database or select and display them.
// 4. Example: 'INSERT 1 John' would insert a row with id=1 and name='John'.
//             'SELECT' would display all inserted rows.
//             'exit' exits the program.
// 5. The program handles file reading/writing, memory allocation, and user input.