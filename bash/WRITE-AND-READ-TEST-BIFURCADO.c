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

// char abc[5] = {'a', 'b', 'c', 'd', 'e'};
int rank, mpi_size;

off_t fsize(const char *filename);

void addMsg(char *msg, char *_summary, char *_header, char *header_msg)
{
    // double time_taken_sum = 0.0;
    // double time_taken = end - start;
    // MPI_Reduce(&time_taken, &time_taken_sum, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    // if (!rank)
    {
        sprintf(_header, "%s,%s", _header, header_msg);
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

int main(int argc, char **argv)
{

    int ret = -1;
    char _stdout[1000] = {0};
    char _summary[1000] = {0};
    char _header[1000] = {0};
    // to measure time.
    double start, end;
    char msg[100];
    // file variables.
    char file_path[100];
    int fd;
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

    // _stdout[0] = '\0';
    // _summary[0] = '>';

    sprintf(_stdout, "[%s][%d]", hostname, rank);

    MPI_Status status;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);
    // fprintf(stderr, "Process  %d of %d is alive\n", rank, mpi_size);

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
    char c;
    // int c = 0;

    if (rank == 0)
        fprintf(stderr, "file_path %s\n", file_path);
    // if (rank == 0)
    //     fprintf(stderr, "[OPERATION],[TIME(ms)]\n");
    long start_position = rank * buffer_size + offset;
    // for (h = 1; h <= n_of_tests; h++)
    sprintf(_stdout, "%s, buffer_size=%ld, start_position=%ld", _stdout, buffer_size, start_position);

    for (size_t iteration = 0; iteration < 10; iteration++)
    {

        sprintf(msg, "%d", rank);
        addMsg(msg, _summary, _header, "Rank");
        // fprintf(stderr, "Iteration %ld\n", iteration);
        sprintf(_stdout, "%s, ITERATION=%ld", _stdout, iteration);
        // get a character.
        c = 48 + rank % 120;
        // c = abc[rank];
        // c = (int)(1 + start_position);
        // fprintf(stderr, "[%d] c=%ld, %ld\n", rank, (97 + start_position), c);
        // fprintf(stderr, "- - -[Test %ld] c:%c %ld - - -\n", h, c, h / n_of_tests);

        // allocate memory.
        buffer_w = (char *)malloc(buffer_size * sizeof(char) + 1);
        // fill the buffer.
        for (b_i = 0; b_i < buffer_size; b_i++)
        {
            // buffer_w[b_i] = abc[rank];
            buffer_w[b_i] = (char)c; // abc[rank];
        }
        // buffer_w[buffer_size - 1] = '\n';
        buffer_w[buffer_size] = '\0';
        // fprintf(stderr, "buffer_w=%s\n", buffer_w);
        // fprintf(stderr, "[%d] buffer_r=%s\n", rank, buffer_r);
        sprintf(msg, "%d", mpi_size);
        addMsg(msg, _summary, _header, "#Process");

        sprintf(msg, "%ld", buffer_size);
        addMsg(msg, _summary, _header, "BufferSize");

        sprintf(msg, "%s", hostname);
        addMsg(msg, _summary, _header, "Hostname");

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
        sprintf(msg, "%f", (end - start));
        addMsg(msg, _summary, _header, "Open-O_CREAT");
        ////sleep(2);
        // close file.

        start = MPI_Wtime();
        close(fd);
        end = MPI_Wtime();
        sprintf(msg, "%f", end - start);
        addMsg(msg, _summary, _header, "Close-O_CREAT");

        // OPEN FILE TO WRITE
        start = MPI_Wtime();
        fd = open(file_path, O_RDWR, 0666);
        if (fd == -1)
        {
            perror("Error opening the file");
            exit(1);
        }
        end = MPI_Wtime();
        sprintf(msg, "%f", end - start);
        addMsg(msg, _summary, _header, "Open-O_RDWR");

        // write into the file.
        // t = clock();
        {
            // WRITE BLOCK.
            // fprintf(stderr, "start_position=%ld\n", start_position);
            start = MPI_Wtime();
            ret = lseek(fd, start_position, SEEK_SET);
            end = MPI_Wtime();
            sprintf(msg, "%f", end - start);
            addMsg(msg, _summary, _header, "Lseek-SEEK_SET");
            // real_write_size = write(fd, buffer_w + start_position, buffer_size);
            // //sleep(rank * 5);
            start = MPI_Wtime();
            real_write_size = write(fd, buffer_w, buffer_size);
            end = MPI_Wtime();
            sprintf(msg, "\x1B[34m%f\033[0m", end - start);
            addMsg(msg, _summary, _header, "\x1B[34mWrite\033[0m");
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
        sprintf(msg, "%f", end - start);
        addMsg(msg, _summary, _header, "Close-O_RDWR");

        // sleep(10);
        // MPI_Barrier(MPI_COMM_WORLD);

        // OPEN FILE TO READ
        start = MPI_Wtime();
        fd = open(file_path, O_RDONLY, 0666);
        // printf("fd=%d\n",fd);
        if (fd == -1)
        {
            perror("Error opening the file");
        }
        end = MPI_Wtime();
        sprintf(msg, "%f", end - start);
        addMsg(msg, _summary, _header, "Open-O_RDONLY");

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
            sprintf(msg, "%f", end - start);
            addMsg(msg, _summary, _header, "Lseek-SEEK_SET");
            // fprintf(stderr, "[CLIENT] %s\n", _stdout);
            start = MPI_Wtime();
            real_read_size = read(fd, buffer_r, buffer_size);
            end = MPI_Wtime();
            // fprintf(stderr,"read=%f\n", end - start);
            sprintf(msg, "\x1B[34m%f\033[0m", end - start);
            addMsg(msg, _summary, _header, "\x1B[34mRead\033[0m");
            // fprintf(stderr, "Read time=%f\n", end - start);
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
        sprintf(msg, "%f\n", end - start);
        addMsg(msg, _summary, _header, "Close-O_RDONLY\n");

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

        // free memory.
        free(buffer_w);
        free(buffer_r);
    }

    fprintf(stderr, "[CLIENT] %s\n", _stdout);
    MPI_Barrier(MPI_COMM_WORLD);
    if (!rank)
    {
        fprintf(stderr, "[Summary] \n%s\n", _header);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    fprintf(stderr, "%s\n", _summary);

    MPI_Finalize();
}

off_t fsize(const char *filename)
{
    struct stat st;

    if (stat(filename, &st) == 0)
        return st.st_size;

    return -1;
}
