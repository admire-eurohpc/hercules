#define _GNU_SOURCE   // Required for openat
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>

void printUsage(char *exe)
{
    printf("Not enough arguments, usage: \n %s <directory_path> <file_name>\n", exe);
}

int main(int argc, char **argv) {

    if (argc != 3)
    {
        printUsage(argv[0]);
        exit(EXIT_FAILURE);
    }

    // Specify the directory path
    //const char *directoryPath = "/path/to/your/directory";
    const char *directoryPath = argv[1];
    // Specify the file name to open or create
    const char *fileName = argv[2];

    // Open the directory and obtain a file descriptor
    int dirfd = open(directoryPath, O_RDONLY);
    if (dirfd == -1) {
        perror("Error opening directory");
        exit(EXIT_FAILURE);
    }

    // Use openat to open or create the file relative to the specified directory
    int filefd = openat(AT_FDCWD, fileName, O_CREAT | O_WRONLY | O_TRUNC, S_IRUSR | S_IWUSR);
    if (filefd == -1) {
        perror("Error opening or creating file");
        close(dirfd);  // Close the directory file descriptor before exiting
        exit(EXIT_FAILURE);
    }

    // Write some content to the file
    const char *content = "Hello, openat!\n";
    ssize_t bytesWritten = write(filefd, content, strlen(content));
    if (bytesWritten == -1) {
        perror("Error writing to file");
        close(filefd);  // Close the file before exiting
        close(dirfd);   // Close the directory file descriptor before exiting
        exit(EXIT_FAILURE);
    }

    printf("File created and written successfully!\n");

    // Close the file and directory file descriptors
    close(filefd);
    close(dirfd);

    return 0;
}
