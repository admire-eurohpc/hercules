#include <stdint.h>
#include <unistd.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <time.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/file.h>
// #include <sys/types.h>
#include "mpi.h"

#define ITERATIONS 1
#define SYSTEM 0

// char abc[5] = {'a', 'b', 'c', 'd', 'e'};
int rank, mpi_size, write_header = 1;

off_t fsize(const char *filename);

void addMsg(char *msg, char *_summary, char *_header, char *header_msg)
{
    // double time_taken_sum = 0.0;
    // double time_taken = end - start;
    // MPI_Reduce(&time_taken, &time_taken_sum, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    // if (!rank)
    {
        if (write_header)
        {
            sprintf(_header, "%s,%s", _header, header_msg);
        }
        // sprintf(_summary, "%s,%5f", _summary, time_taken_sum / (double)mpi_size);
        sprintf(_summary, "%s,%s", _summary, msg);
    }
}

uint32_t MurmurOAAT32(const char *key)
{
    uint32_t h = 335ul;
    for (; *key; ++key)
    {
        h ^= *key;
        h *= 0x5bd1e995;
        h ^= h >> 15;
    }
    return abs(h);
}

void printUsage(char *exe)
{
    printf("Not enough arguments, usage: \n %s <directory_path> <file_name> <buffer_size>", exe);
}

int main(int argc, char **argv)
{

    if (argc != 4)
    {
        printUsage(argv[0]);
        exit(EXIT_FAILURE);
    }

    int ret = -1;
    char err_msg[100] = {0};
    char _stdout[10000] = {0};
    char _summary[10000] = {0};
    char _header[10000] = {0};
    // to measure time.
    double start, end;
    char msg[100];
    // file variables.
    char file_name[100], file_path[100], dir_path[100];
    int fd; // file descriptor.
    // Getting a mostly unique id for the distributed deployment.
    char hostname[1024];
    char hostname_pid[1024];

    ret = gethostname(&hostname[0], 512);
    if (ret == -1)
    {
        perror("gethostname");
        exit(EXIT_FAILURE);
    }
    sprintf(hostname_pid, "%s:%d", hostname, getpid());
    rank = MurmurOAAT32(hostname_pid);

    sprintf(_stdout, "[%s][%d]", hostname, rank);

    MPI_Status status;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);
    fprintf(stderr, "Process  %d of %d is alive\n", rank + 1, mpi_size);

    strcpy(dir_path, argv[1]);                            // path of the directory.
    strcpy(file_name, argv[2]);                           // name of the file.
    size_t final_file_size = atoi(argv[3]) * 1024 * 1024; // file size in MB.
    size_t buffer_size = 0;

    off_t offset = 0, curr_offset = -1;
    char *buffer_w = NULL, *buffer_r = NULL;

    size_t real_read_size = 0, real_write_size = 0;
    int b_i;
    size_t h, i, j;
    char c;

    size_t residue = final_file_size % mpi_size;
    buffer_size = final_file_size / mpi_size;

    if (!rank)
        fprintf(stderr, "dir_path=%s, file_name=%s, final_file_size=%ld, buffer_size=%ld\n", dir_path, file_name, final_file_size, buffer_size);

    size_t start_position = 0;

    start_position = rank * buffer_size + offset;

    // if (rank + 1 == mpi_size)
    // {
    //     buffer_size += residue;
    // }

    // fprintf(stderr,"start_position=%ld\n", start_position);

    sprintf(_stdout, "%s, buffer_size=%ld, start_position=%ld", _stdout, buffer_size, start_position);

    FILE *fp;

    // if (!rank)
    //     sleep(1);

    for (size_t iteration = 1; iteration <= ITERATIONS; iteration++)
    {

        sprintf(msg, "%d", rank);
        addMsg(msg, _summary, _header, "Rank");
        // fprintf(stderr, "Iteration %ld\n", iteration);
        sprintf(_stdout, "%s, ITERATION=%ld", _stdout, iteration);
        // get a character.
        c = 48 + rank + iteration % 120;

        // allocate memory.
        buffer_w = (char *)malloc(buffer_size * sizeof(char) + 1);
        // fill the buffer.
        for (b_i = 0; b_i < buffer_size; b_i++)
        {
            buffer_w[b_i] = (char)c;
        }

        buffer_w[buffer_size] = '\0';

        sprintf(msg, "%d", mpi_size);
        addMsg(msg, _summary, _header, "#Process");

        sprintf(msg, "%ld", buffer_size);
        addMsg(msg, _summary, _header, "BufferSize");

        sprintf(msg, "%s", hostname);
        addMsg(msg, _summary, _header, "Hostname");

        // CREATE THE DIRECTORY.
        if (!rank)
        {
            int istat;
            istat = mkdir(dir_path, 0755);

            if (istat < 0)
            {
                sprintf(err_msg, "[%ld] Error creating the directory", iteration);
                perror(err_msg);
                // exit(1);
            }
        }

        if (iteration)
        {
            // sprintf(file_path, "%s/%s-iteration%ld-rank%d", dir_path, file_name, iteration, rank);
            sprintf(file_path, "%s/%s", dir_path, file_name);
        }

        if (!rank)
            fprintf(stderr, "[%ld] absolute_file_path=%s\n", iteration, file_path);

        // CREATE THE FILE.
        start = MPI_Wtime();

        ret = 0;
        // if (!rank)
        {
            if (SYSTEM)
            {
                fd = open(file_path, O_CREAT, 0755);
                if (fd == -1)
                {
                    ret = -1;
                }
            }
            else
            {
                fp = fopen(file_path, "w");
                // fprintf(stderr, "\tmode=%d\n", fp->_mode);
                if (fp == NULL)
                {
                    ret = -1;
                }
            }

            if (ret == -1)
            {
                perror("Error opening the file");
                exit(EXIT_FAILURE);
            }
            else
            {
                fprintf(stderr, "+++ File created, %d:%s +++\n", errno, strerror(errno));
            }
        }

        end = MPI_Wtime();
        sprintf(msg, "%f", (end - start));
        addMsg(msg, _summary, _header, "Open-O_CREAT");

        // CLOSE FILE.
        start = MPI_Wtime();
        // if (!rank)
        {
            if (SYSTEM)
            {
                close(fd);
            }
            else
            {
                fclose(fp);
            }
        }
        end = MPI_Wtime();
        sprintf(msg, "%f", end - start);
        addMsg(msg, _summary, _header, "Close-O_CREAT");

        // OPEN FILE TO WRITE.
        start = MPI_Wtime();
        ret = 0;
        if (SYSTEM)
        {
            fd = open(file_path, O_RDWR, 0755);
            if (fd == -1)
            {
                ret = -1;
            }
        }
        else
        {
            fp = fopen(file_path, "w");
            if (fp == NULL)
            {
                ret = -1;
            }
        }

        if (ret == -1)
        {
            perror("Error opening the file");
            exit(EXIT_FAILURE);
        }
        else
        {
            fprintf(stderr, "+++ File opened for writting, start_position=%ld, %d:%s +++\n", start_position, errno, strerror(errno));
        }

        end = MPI_Wtime();
        sprintf(msg, "%f", end - start);
        addMsg(msg, _summary, _header, "Open-O_RDWR");

        // Use flock to lock the file for writing
        // flock(fd, LOCK_EX);
        // Write into the file.
        {
            // Move the pointer to the start position.
            start = MPI_Wtime();
            ret = 0;
            // fprintf(stderr, "start_position=%ld", start_position);
            if (SYSTEM)
            {
                ret = lseek(fd, start_position, SEEK_SET);
            }
            else
            {
                ret = lseek(fp->_fileno, start_position, SEEK_SET);
            }
            if (ret == -1)
            {
                fprintf(stderr, "Error during lseek for writing: %d:%s\n", errno, strerror(errno));
                exit(EXIT_FAILURE);
            }
            else
            {
                fprintf(stderr, "Lseek, ret=%d: %d:%s\n", ret, errno, strerror(errno));
            }
            end = MPI_Wtime();
            sprintf(msg, "%f", end - start);
            addMsg(msg, _summary, _header, "Lseek-SEEK_SET");

            // WRITE.
            start = MPI_Wtime();
            ret = 0;
            if (SYSTEM)
            {
                real_write_size = write(fd, buffer_w, buffer_size);
                if (real_write_size != buffer_size)
                {
                    char error[500];
                    sprintf(error, "[%d][Test %ld] error write, write size: %ld/%ld\n", rank, h, real_write_size, buffer_size);
                    perror(error);
                    ret = -1;
                }
            }
            else
            {
                fwrite(buffer_w, sizeof(buffer_w[0]), buffer_size, fp);
                if (ferror(fp))
                {
                    char error[500];
                    sprintf(error, "[%d][Test %ld] Error writing %s\n", rank, h, file_path);
                    perror(error);
                    ret = -1;
                }
                else if (feof(fp))
                {
                    char error[500];
                    sprintf(error, "[%d][Test %ld] EOF found while writing %s\n", rank, h, file_path);
                    perror(error);
                    ret = -1;
                }
            }
            if (ret == -1)
            {
                exit(EXIT_FAILURE);
            }
            else
            {
                fprintf(stderr, "Write: %d:%s\n", errno, strerror(errno));
            }
            end = MPI_Wtime();

            sprintf(msg, "\x1B[34m%f\033[0m", end - start);
            addMsg(msg, _summary, _header, "\x1B[34mWrite\033[0m");
        }
        // Release the file lock
        // flock(fd, LOCK_UN);

        // CLOSE FILE.
        start = MPI_Wtime();
        if (SYSTEM)
        {
            close(fd);
        }
        else
        {
            fclose(fp);
        }

        end = MPI_Wtime();
        sprintf(msg, "%f", end - start);
        addMsg(msg, _summary, _header, "Close-O_RDWR");

        // sleep(10);
        // MPI_Barrier(MPI_COMM_WORLD);

        // OPEN FILE TO READ.
        start = MPI_Wtime();
        ret = 0;
        if (SYSTEM)
        {
            fd = open(file_path, O_RDONLY, 0755);
            if (fd == -1)
            {
                ret = -1;
            }
        }
        else
        {
            fp = fopen(file_path, "r");
            if (fp == NULL)
            {
                ret = -1;
            }
        }

        if (ret == -1)
        {
            perror("Error opening the file");
            exit(EXIT_FAILURE);
        }
        else
        {
            fprintf(stderr, "+++ File opened for reading, start_position=%ld +++\n", start_position);
        }

        end = MPI_Wtime();
        sprintf(msg, "%f", end - start);
        addMsg(msg, _summary, _header, "Open-O_RDONLY");

        // allocate memory.
        buffer_r = (char *)malloc(buffer_size * sizeof(char) + 1);
        buffer_r[buffer_size] = '\0';

        // read file.
        {
            start = MPI_Wtime();
            // Move the pointer to the start position.
            ret = 0;
            if (SYSTEM)
            {
                ret = lseek(fd, start_position, SEEK_SET);
            }
            else
            {
                ret = lseek(fp->_fileno, start_position, SEEK_SET);
            }
            if (ret == -1)
            {
                fprintf(stderr, "Error during lseek for reading: %d:%s\n", errno, strerror(errno));
                exit(EXIT_FAILURE);
            }
            end = MPI_Wtime();
            sprintf(msg, "%f", end - start);
            addMsg(msg, _summary, _header, "Lseek-SEEK_SET");

            start = MPI_Wtime();
            // READ BLOCK.
            ret = 0;
            if (SYSTEM)
            {
                real_read_size = read(fd, buffer_r, buffer_size);
                if (real_read_size != buffer_size)
                {
                    fprintf(stderr, "[%d] Error during reading - %d:%s\n", rank, errno, strerror(errno));
                    char error[500];
                    sprintf(error, "[%d][Test %ld] error read, read size: %ld/%ld\n", rank, h, real_read_size, buffer_size);
                    perror(error);
                    ret = -1;
                }
            }
            else
            {
                fread(buffer_r, sizeof(buffer_r[0]), buffer_size, fp);
                if (ferror(fp))
                {
                    char error[500];
                    sprintf(error, "[%d][Test %ld] Error reading %s\n", rank, h, file_path);
                    perror(error);
                    ret = -1;
                }
                else if (feof(fp))
                {
                    char error[500];
                    sprintf(error, "[%d][Test %ld] EOF found while reading %s\n", rank, h, file_path);
                    perror(error);
                    ret = -1;
                }
            }
            if (ret == -1)
            {
                exit(EXIT_FAILURE);
            } else
            {
                fprintf(stderr, "Read: %d:%s\n", errno, strerror(errno));
            }
            end = MPI_Wtime();

            sprintf(msg, "\x1B[34m%f\033[0m", end - start);
            addMsg(msg, _summary, _header, "\x1B[34mRead\033[0m");

            // VERIFY CURRENT OFFSET.
            ret = 0;
            if (SYSTEM)
            {
                curr_offset = lseek(fd, 0, SEEK_CUR);
            }
            else
            {
                // Get the current offset using ftell.
                curr_offset = ftell(fp);
            }

            if (offset == (off_t)-1)
            {
                perror("Error getting file offset");
            }
            else
            {
                fprintf(stderr, "Current offset after read: %ld\n", curr_offset);
            }
        }

        // CLOSE FILE.
        start = MPI_Wtime();
        if (SYSTEM)
        {
            close(fd);
        }
        else
        {
            fclose(fp);
        }
        end = MPI_Wtime();
        sprintf(msg, "%f\n", end - start);
        addMsg(msg, _summary, _header, "Close-O_RDONLY\n");

        int result = 0;
        size_t count_differences = 0;

        fprintf(stderr, "Verifying buffers\n");
        for (size_t i = 0; i < buffer_size; i++)
        {
            if (buffer_w[i] != buffer_r[i])
            {
                result = 1;
                count_differences++;
            }
        }
        sprintf(_stdout, "%s, count_differences=%ld\n", _stdout, count_differences);

        // sprintf(_stdout, "%s, size of buffer_r %ld", _stdout, strlen(buffer_r));
        // fprintf(stderr, "strcmp = %d\n", result);
        if (result)
        {
            strcat(_stdout, "\t\x1B[31mWrite and Read buffer are different!\033[0m\n");
            // fprintf(stderr, "Write and Read buffer are different!\n");
            // fprintf(stderr, "buffer_w=%s\n", buffer_w);
            // fprintf(stderr, "buffer_r=%s\n", buffer_r);
        }
        else
        {
            strcat(_stdout, "\t\x1B[32mWrite and Read buffer are equals!\033[0m\n");
            // fprintf(stderr, "buffer_w=%s\n", buffer_w);
            // fprintf(stderr, "buffer_r=%s\n", buffer_r);
        }

        // free memory.
        fprintf(stderr, "Deleting buffers\n");
        free(buffer_w);
        free(buffer_r);
        write_header = 0;
    } // end for

    fprintf(stderr, "[CLIENT] %s\n", _stdout);
    MPI_Barrier(MPI_COMM_WORLD);
    if (!rank)
    {
        fprintf(stderr, "[Summary] \n%s\n", _header);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    fprintf(stderr, "%s", _summary);

    MPI_Finalize();
}

off_t fsize(const char *filename)
{
    struct stat st;

    if (stat(filename, &st) == 0)
        return st.st_size;

    return -1;
}
