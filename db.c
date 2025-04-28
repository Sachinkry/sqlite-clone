#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define PAGE_SIZE 4096

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

    // Initialize the buffer with zeros if new file
    if (bytesRead == 0)
    {
        memset(buffer, 0, PAGE_SIZE);
        printf("Buffer initialized with zeros\n");
    }

    // Simple REPL loop
    char input[100];
    while (1)
    {
        printf("db>");
        fflush(stdout);
        if (fgets(input, sizeof(input), stdin) == NULL)
        {
            break; // EOF or error
        }
        printf("You entered: %s", input);
    }

    // Cleanup
    free(buffer);
    fclose(file);
    printf("File closed successfully\n");
    return 0;
}

// summary:
// 1. The code opens a database file named "mydb.db". If not found, it creates a new one.
// 2. It allocates a buffer of size PAGE_SIZE (4096 bytes) to read data from the file.
// 3. Simple REPL loop allows user input and echoes it back.
