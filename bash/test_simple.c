#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/time.h>
#include <fcntl.h>
#include <strings.h>
#define KB 1024
int main ( int argc, char **argv) {


	int fd;
	struct timeval start, end;
	u_int64_t delta_us;

	//int size=1024*1024*10;
	int size=1024*1024*2;
	char *  buffer=malloc(size);
	int i;
//aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa...
	for(i=0;i<size;i++){
		buffer[i]='a';
	}	

	fd = open (argv[1], O_CREAT | O_RDWR, 0666);
	//printf("fd=%d\n",fd);
       	if(fd==-1){
		perror("Error opening the file");
	}	
	
	gettimeofday(&start, NULL);

	write (fd,buffer,size);
       	close (fd);

	gettimeofday(&end, NULL);

	delta_us = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
	printf("Test:  size=%d KB. WRITE Execution time %lu ms.  TP %6.1f MB/s \n",size/KB, delta_us,( (size/((float) (KB*KB)))/(delta_us / 1000.0F)));

	
	gettimeofday(&start, NULL);


/*read*/ 	
	fd = open (argv[1],  O_RDWR, 0666);
	if(fd==-1){	
		perror("Error opening the file:");
	}
        int err=read (fd,buffer,size);
       	if(err!=size){
		perror("error read");
	}else{
		printf("Salida:%c\n",buffer[1]);
	}
	close (fd);

	gettimeofday(&end, NULL);
	
	delta_us = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
	printf("Test:  size=%d KB. READ Execution time %lu ms.  TP %6.1f MB/s \n",size/KB, delta_us,( (size/((float) (KB*KB)))/(delta_us / 1000.0F)));


//bb
	int size2=2;
 	//bzero(buffer,size*sizeof(char*));
	free(buffer);
	buffer=malloc(size2);
	for(i=0;i<size2;i++){
                buffer[i]='b';
        }

        fd = open (argv[1], O_CREAT | O_RDWR, 0666);
        //printf("fd=%d\n",fd);
        if(fd==-1){
                perror("Error opening the file");
        }

        gettimeofday(&start, NULL);

        write (fd,buffer,size2);
        close (fd);

        gettimeofday(&end, NULL);

        delta_us = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
        printf("Test:  size=%d KB. WRITE Execution time %lu ms.  TP %6.1f MB/s \n",size2/KB, delta_us,( (size2/((float) (KB*KB)))/(delta_us / 1000.0F)));

/*read*/
	free(buffer);
        buffer=malloc(size);

        gettimeofday(&start, NULL);

        fd = open (argv[1],  O_RDWR, 0666);
        if(fd==-1){
                perror("Error opening the file:");
        }
        err=read (fd,buffer,size);
        if(err!=size){
                printf("File is smaller than requested. Read only read=%d\n",err);
        }else{
                printf("Salida:%c\n",buffer[1]);
                printf("Salida:%c\n",buffer[3]);
        }
        close (fd);

        gettimeofday(&end, NULL);

        delta_us = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
        printf("Test:  size=%d KB. READ Execution time %lu ms.  TP %6.1f MB/s \n",size2/KB, delta_us,( (size2/((float) (KB*KB)))/(delta_us / 1000.0F)));

}
