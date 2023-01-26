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

off_t fsize(const char *filename);

int main(int argc, char **argv)
{

    int rank, mpi_size;
    char _stdout[1000];

    _stdout[0] = '\0';

    MPI_Status status;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);
    // fprintf(stderr, "Process  %d of %d is alive\n", rank, amount);

    // to measure time.
    clock_t t;
    double time_taken;
    // int ret = -1;

    char file_path[100];
    int fd;

    strcpy(file_path, argv[1]); // path/name of the file.
    // int n_of_tests = atoi(argv[2]);           // number of tests to do.
    // size_t file_size = atoi(argv[2]) * 1024; // size of every block to be write/read (kilobytes).
    // int n_of_blocks = atoi(argv[4]);          // number of blocks to be write/read.

    // size_t buffer_size = file_size / mpi_size;
    // size_t buffer_size = file_size;
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
    long start_position = rank * buffer_size;
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
        buffer_w = (char *)malloc(buffer_size * sizeof(char)+1);
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
        sleep(1 * rank);
        fd = open(file_path, O_CREAT, 0666);
        if (fd == -1)
        {
            perror("Error opening the file");
            exit(1);
        }
        ////sleep(2);
        // close file.
        close(fd);

        // MPI_Finalize();
        // return 0;

        // OPEN FILE TO WRITE
        fd = open(file_path, O_RDWR, 0666);
        if (fd == -1)
        {
            perror("Error opening the file");
            exit(1);
        }

        // write into the file.
        t = clock();
        {
            // WRITE BLOCK.
            // fprintf(stderr, "start_position=%ld\n", start_position);
            lseek(fd, start_position, SEEK_SET);
            // real_write_size = write(fd, buffer_w + start_position, buffer_size);
            // //sleep(rank * 5);
            real_write_size = write(fd, buffer_w, buffer_size);

            // lseek(fd, start_position, SEEK_SET);

            // fprintf(stderr, "[WRITE] i=%ld/%ld\n", i, real_write_size);
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
        ////sleep(2);
         // close file.
        close(fd);

        //sleep(2);

        // OPEN FILE TO READ
        fd = open(file_path, O_RDONLY, 0666);
        // printf("fd=%d\n",fd);
        if (fd == -1)
        {
            perror("Error opening the file");
        }

        // //sleep(10);

        // allocate memory.
        buffer_r = (char *)malloc(buffer_size * sizeof(char)+1);
        buffer_r[buffer_size] = '\0';

        // read file.
        {
            // READ BLOCK.
            lseek(fd, start_position, SEEK_SET);
            real_read_size = read(fd, buffer_r, buffer_size);
            if (real_read_size != buffer_size)
            {
                char error[500];
                sprintf(error, "[Test %ld] error read, readed size: %ld/%ld\n", h, real_read_size, buffer_size);
                perror(error);
                exit(1);
            }
            // fprintf(stderr, "real_read_size=%ld\n", real_read_size);
            sprintf(_stdout, "%s, real_read_size=%ld\n", _stdout, real_read_size);

            // else
            // {
            //     fprintf(stderr, "[Test %ld] Ok! readed size: %d: %s\n", h, real_read_size, buffer);
            //     fprintf(stderr, "[READ] i=%ld/%ld\n", i + file_size, buffer_size);
            // }
        }

        // close file.
        close(fd);

        //sleep(2);

        int result = 0;//strcmp(buffer_w, buffer_r);


        for (size_t i = 0; i < buffer_size; i++)
        {
            if (buffer_w[i] != buffer_r[i])
            {
                result = 1;
            }  
        }
        
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
        // fprintf(stderr, "buffer_w=%s\n", buffer_w+buffer_size-1024);
        // fprintf(stderr, "buffer_r=%s\n", buffer_r+buffer_size-1024);

        t = clock() - t;
        time_taken = ((double)t) / (CLOCKS_PER_SEC / 1000);
        // fprintf(stderr, "[%d][WRITE],%5f\n", rank, time_taken);
        // MPI_Barrier(MPI_COMM_WORLD);
        // //sleep(2);
        // sprintf(_stdout, "%s, total_file_size=%ld", _stdout, fsize(file_path));
        fprintf(stderr, "[CLIENT] %s\n", _stdout);

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