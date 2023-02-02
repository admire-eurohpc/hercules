#include <unistd.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <time.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include "mpi.h"

char abc[5] = {'a', 'b', 'c', 'd', 'e'};
int rank, mpi_size;

off_t fsize(const char *filename);

void addTime(double start, double end, char *_summary, char *_header, char *msg)
{
    double time_taken_sum = 0.0;
    double time_taken = end - start;
    MPI_Reduce(&time_taken, &time_taken_sum, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    if (!rank)
    {
        sprintf(_header, "%s \x1B[34m %s \033[0m", _header, msg);
        sprintf(_summary, "%s \x1B[34m %5f \033[0m", _summary, time_taken_sum / (double)mpi_size);
    }
}

int main(int argc, char **argv)
{

    char _stdout[1000];
    char _summary[1000];
    char _header[1000];

    _stdout[0] = '\0';
    _summary[0] = '\0';

    MPI_Status status;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);
    fprintf(stderr, "Process  %d of %d is alive\n", rank, mpi_size);

    // to measure time.
    // clock_t t;
    double start, end;
    double time_taken;
    int ret = -1;

    char file_path[100];
    int fd;

    strcpy(file_path, argv[1]); // path/name of the file.
    // int n_of_tests = atoi(argv[2]);           // number of tests to do.
    // size_t file_size = atoi(argv[2]) * 1024; // size of every block to be write/read (kilobytes).
    // int n_of_blocks = atoi(argv[4]);          // number of blocks to be write/read.

    // size_t buffer_size = file_size / mpi_size;
    // size_t buffer_size = file_size;
    // size_t offset = 1024;
    size_t offset = 0;
    size_t buffer_size = atoi(argv[2]) * 1024;
    // fprintf(stderr, "buffer_size=%ld\n", buffer_size);
    char *buffer_w = NULL, *buffer_r = NULL;
    // int read_size_idx_j;
    size_t real_read_size = 0, real_write_size = 0;
    int b_i;
    size_t h, i, j;
    // char c;
    // int c = 0;

    if (rank == 0)
        fprintf(stderr, "file_path %s\n", file_path);
    // if (rank == 0)
    //     fprintf(stderr, "[OPERATION],[TIME(ms)]\n");
    long start_position = rank * buffer_size + offset;
    // for (h = 1; h <= n_of_tests; h++)
    sprintf(_stdout, "buffer_size=%ld, start_position=%ld", buffer_size, start_position);

    {
        // get a character.
        // c = 48 + rank;
        // c = abc[rank];
        // c = (int)(1 + start_position);
        // fprintf(stderr, "[%d] c=%ld, %ld\n", rank, (97 + start_position), c);
        // fprintf(stderr, "- - -[Test %ld] c:%c %ld - - -\n", h, c, h / n_of_tests);

        // allocate memory.
        buffer_w = (char *)malloc(buffer_size * sizeof(char) + 1);
        // fill the buffer.
        for (b_i = 0; b_i < buffer_size; b_i++)
        {
            // buffer_w[b_i] = (char)c++;
            buffer_w[b_i] = abc[rank];
        }
        // buffer_w[buffer_size - 1] = '\n';
        buffer_w[buffer_size] = '\0';
        // fprintf(stderr, "buffer_w=%s\n", buffer_w);
        // fprintf(stderr, "[%d] buffer_r=%s\n", rank, buffer_r);

        // CREATE THE FILE.
        // fd = creat(file_path, 0666);
        // sleep(1 * rank);
        start = MPI_Wtime();
        fd = open(file_path, O_CREAT, 0666);
        if (fd == -1)
        {
            perror("Error opening the file");
            exit(1);
        }
        end = MPI_Wtime();
        addTime(start, end, _summary, _header, "Open-O_CREAT");
        ////sleep(2);
        // close file.

        start = MPI_Wtime();
        close(fd);
        end = MPI_Wtime();
        addTime(start, end, _summary, _header, "Close-O_CREAT");

        // OPEN FILE TO WRITE
        start = MPI_Wtime();
        fd = open(file_path, O_RDWR, 0666);
        if (fd == -1)
        {
            perror("Error opening the file");
            exit(1);
        }
        end = MPI_Wtime();
        addTime(start, end, _summary, _header, "Open-O_RDWR");

        // write into the file.
        // t = clock();
        {
            // WRITE BLOCK.
            // fprintf(stderr, "start_position=%ld\n", start_position);
            start = MPI_Wtime();
            ret = lseek(fd, start_position, SEEK_SET);
            end = MPI_Wtime();
            addTime(start, end, _summary, _header, "Lseek-SEEK_SET");
            // real_write_size = write(fd, buffer_w + start_position, buffer_size);
            // //sleep(rank * 5);
            start = MPI_Wtime();
            real_write_size = write(fd, buffer_w, buffer_size);
            end = MPI_Wtime();
            addTime(start, end, _summary, _header, "Write");
            if (real_write_size != buffer_size)
            {
                char error[500];
                sprintf(error, "[%d][Test %ld] error write, write size: %ld/%ld\n", rank, h, real_write_size, buffer_size);
                perror(error);
                exit(1);
            }
            // fprintf(stderr, "real_write_size=%ld\n", real_write_size);
            sprintf(_stdout, "%s, real_write_size=%ld", _stdout, real_write_size);
        }
        // close file.
        start = MPI_Wtime();
        close(fd);
        end = MPI_Wtime();
        addTime(start, end, _summary, _header, "Close-O_RDWR");

        // sleep(10);

        // OPEN FILE TO READ
        start = MPI_Wtime();
        fd = open(file_path, O_RDONLY, 0666);
        // printf("fd=%d\n",fd);
        if (fd == -1)
        {
            perror("Error opening the file");
        }
        end = MPI_Wtime();
        addTime(start, end, _summary, _header, "Open-O_RDONLY");

        sleep(5);

        // allocate memory.
        buffer_r = (char *)malloc(buffer_size * sizeof(char) + 1);
        buffer_r[buffer_size] = '\0';

        // fprintf(stderr, "buffer_r address=%p\n", buffer_r);

        // read file.
        {
            // READ BLOCK.
            start = MPI_Wtime();
            lseek(fd, start_position, SEEK_SET);
            end = MPI_Wtime();
            addTime(start, end, _summary, _header, "Lseek-SEEK_SET");
            // fprintf(stderr, "[CLIENT] %s\n", _stdout);
            start = MPI_Wtime();
            real_read_size = read(fd, buffer_r, buffer_size);
            end = MPI_Wtime();
            addTime(start, end, _summary, _header, "Read");
            if (real_read_size != buffer_size)
            {
                fprintf(stderr, "[CLIENT] %s\n", _stdout);
                char error[500];
                sprintf(error, "[Test %ld] error read, readed size: %ld/%ld\n", h, real_read_size, buffer_size);
                perror(error);
                exit(1);
            }
            // fprintf(stderr, "real_read_size=%ld\n", real_read_size);
            sprintf(_stdout, "%s, real_read_size=%ld", _stdout, real_read_size);
        }

        // close file.
        start = MPI_Wtime();
        close(fd);
        end = MPI_Wtime();
        addTime(start, end, _summary, _header, "Close-O_RDONLY");

        int result = 0; // strcmp(buffer_w, buffer_r);
        size_t count_differences = 0;

        for (size_t i = 0; i < buffer_size; i++)
        {
            if (buffer_w[i] != buffer_r[i])
            {
                // fprintf(stderr, "\x1B[31m[%c] != [%c] pos=%ld\033[0m\n", buffer_w[i], buffer_r[i], i);
                result = 1;
                count_differences++;
                // break;
            }
            else
            {
                // fprintf(stderr, "[%c] == [%c] pos=%ld\n", buffer_w[i], buffer_r[i], i);
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

        fprintf(stderr, "[CLIENT] %s\n", _stdout);

        MPI_Barrier(MPI_COMM_WORLD);
        if (!rank)
        {
            fprintf(stderr, "[Summary] \n%s\n%s\n", _header,_summary);
        }

        // free memory.
        free(buffer_w);
        free(buffer_r);
    }

    MPI_Finalize();
}

off_t fsize(const char *filename)
{
    struct stat st;

    if (stat(filename, &st) == 0)
        return st.st_size;

    return -1;
}