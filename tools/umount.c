#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <syslog.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdlib.h>
#include <time.h>
#include <limits.h>


int main () {
   char buff[16];
   int pid;

   //int run = open("/var/run/imss.pid", O_RDONLY);
   int run = open ("/home/hcristobal/imss/build/imss.pid", O_RDONLY);
   read(run, buff, 16);

   sscanf(buff, "%u" , &pid);

   close(run);
   //unlink("/var/run/imss.pid");
   unlink("/home/hcristobal/imss/build/imss.pid");
   printf("KILL %d\n",pid);
   kill(pid, SIGKILL);


   return 1;
}
