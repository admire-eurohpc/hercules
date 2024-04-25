#include "slog.h"

#ifdef DARWIN

/*
 * Bellow we provide an alternative for clock_gettime,
 * which is not implemented in Mac OS X.
 */
static inline int clock_gettime(int clock_id, struct timespec *ts)
{
    struct timeval tv;

    if (clock_id != CLOCK_REALTIME)
    {
        errno = EINVAL;
        return -1;
    }
    if (gettimeofday(&tv, NULL) < 0)
    {
        return -1;
    }
    ts->tv_sec = tv.tv_sec;
    ts->tv_nsec = tv.tv_usec * 1000;
    return 0;
}

#endif /* DARWIN */

/*
 * Get system date-time.
 */
void slog_get_date(SlogDate *sdate)
{
    time_t rawtime;
    struct tm timeinfo;
    struct timespec now;
    rawtime = time(NULL);
    localtime_r(&rawtime, &timeinfo);

    /* Get System Date-time. */
    sdate->year = timeinfo.tm_year + 1900;
    sdate->mon = timeinfo.tm_mon + 1;
    sdate->day = timeinfo.tm_mday;
    sdate->hour = timeinfo.tm_hour;
    sdate->min = timeinfo.tm_min;
    sdate->sec = timeinfo.tm_sec;

    /* Get micro seconds. */
    clock_gettime(CLOCK_MONOTONIC, &now);
    sdate->usec = now.tv_nsec / 10000000;
}

/*
 * Return program version.
 */
const char *slog_version(int min)
{
    static char verstr[128];

    if (min)
    { /* Get only version numbers. (eg. 1.4.85) */
        sprintf(verstr, "%d.%d.%d", SLOGVERSION_MAJOR, SLOGVERSION_MINOR, SLOGBUILD_NUM);
    }
    else
    { /* Get version in full format. eg 1.4 build 85 (Jan 21 2017). */
        sprintf(verstr, "%d.%d build %d (%s)",
                SLOGVERSION_MAJOR, SLOGVERSION_MINOR, SLOGBUILD_NUM, __DATE__);
    }

    return verstr;
}

/*
 * Colorize the given string (str).
 */
char *strclr(const char *clr, char *str, ...)
{
    static char output[MAXMSG + 120];
    char string[MAXMSG];

    /* Read args. */
    va_list args;
    va_start(args, str);
    vsprintf(string, str, args);
    va_end(args);

    /* Colorize string. */
    sprintf(output, "%s%s%s", clr, string, CLR_RESET);

    return output;
}

/*
 * Append log info to log file.
 */
void slog_to_file(char *out, const char *fname, SlogDate *sdate)
{
    char filename[PATH_MAX];

    if (slg.filestamp)
    { /* Create log filename with date. (eg example-2017-01-21.log) */
        snprintf(filename, sizeof(filename), "%s-%02d-%02d-%02d.log",
                 fname, sdate->year, sdate->mon, sdate->day);
    }
    else
    { /* Create log filename using regular name. (eg example.log) */
        snprintf(filename, sizeof(filename), "%s.log", fname);
    }

    FILE *fp = NULL;
    // fprintf(stderr, "[SLOG] filename='%s'\n", filename);
    // if (fp == NULL)
    // {
    fp = fopen(filename, "a");
    // }

    if (fp == NULL)
    {
        fprintf(stderr, "[SLOG] Error opening file='%s'\n", filename);
        return;
    }

    /* Append log line to log file. */
    fprintf(fp, "%s", out);

    fclose(fp);
}

/*
 * Read cfg file and parse configuration options.
 */
int parse_config(const char *cfg_name)
{
    FILE *fp;
    char *line = NULL;
    size_t len = 0;
    ssize_t read;
    int ret = 0;

    fp = fopen(cfg_name, "r");
    if (fp == NULL)
    {
        return 0;
    }

    /* Reading *.cfg file line-by-line. */
    while ((read = getline(&line, &len, fp)) != -1)
    {
        /* Find level in file. */
        if (strstr(line, "LOGLEVEL") != NULL)
        {
            /* Get max log level to print in stdout. */
            slg.level = atoi(line + 8);
            ret = 1;
        }
        if (strstr(line, "LOGFILELEVEL") != NULL)
        {
            /* Level required to write in file. */
            slg.file_level = atoi(line + 12);
            ret = 1;
        }
        else if (strstr(line, "LOGTOFILE") != NULL)
        {
            /*
             * Get max log level to write in file.
             * If 0 will not write to file.
             */
            slg.to_file = atoi(line + 9);
            ret = 1;
        }
        else if (strstr(line, "PRETTYLOG") != NULL)
        {
            /* If 1 will output with color. */
            slg.pretty = atoi(line + 9);
            ret = 1;
        }
        else if (strstr(line, "FILESTAMP") != NULL)
        {
            /* If 1 will add date to log name. */
            slg.filestamp = atoi(line + 9);
            ret = 1;
        }
    }

    /* Cleanup. */
    if (line)
    {
        free(line);
    }
    fclose(fp);

    return ret;
}

/*
 * Generating string in form:
 * yyyy.mm.dd-HH:MM:SS.UU - (some message).
 */
char *slog_get(SlogDate *pDate, char *msg, ...)
{
    static char output[MAXMSG + 120];
    char string[MAXMSG];

    /* Read args. */
    va_list args;
    va_start(args, msg);
    vsprintf(string, msg, args);
    va_end(args);

    /* Generate output string with date. */
    sprintf(output, "%02d.%02d.%02d-%02d:%02d:%02d.%02d - %s",
            pDate->year, pDate->mon, pDate->day, pDate->hour,
            pDate->min, pDate->sec, pDate->usec, string);

    return output;
}

/*
 * Log exiting process. We save log in file
 * if LOGTOFILE flag is enabled from config.
 */
void slog(int flag, char const *caller_name, const char *msg, ...)
{
    int prev_errno = errno;

    if (flag > slg.level)
    {
        return;
    }

    /* Lock thread for safe. */
    if (slg.td_safe)
    {
        int rc;
        if ((rc = pthread_mutex_lock(&slog_mutex)))
        {
            fprintf(stderr, "[ERROR][%s] <%s:%d> inside %s(): Can not lock mutex: %s\n",
                    slg.fname, __FILE__, __LINE__, __func__, strerror(rc));
            exit(EXIT_FAILURE);
        }
    }

    SlogDate mdate;
    char string[MAXMSG + 120];
    char in_string[MAXMSG];
    char prints[MAXMSG + 240];
    char color[32], alarm[32];
    char *output;

    slog_get_date(&mdate);
    /* Place zero-valued bytes. */
    bzero(string, sizeof(string));
    bzero(in_string, sizeof(in_string));
    bzero(prints, sizeof(prints));
    bzero(color, sizeof(color));
    bzero(alarm, sizeof(alarm));

    /* Read args. */
    va_list args;
    va_start(args, msg);
    vsprintf(in_string, msg, args);
    va_end(args);

    // if(slg.rank!=-1)
    sprintf(string, "[%d][%ld][%d:%s][%s]\t>\t%s", getpid(), pthread_self(), errno, strerror(errno), caller_name, in_string);

    /* Check logging levels. */
    if (flag <= slg.level || flag <= slg.file_level)
    {
        /* Handle flags. */
        switch (flag)
        {
        case SLOG_LIVE:
            strncpy(color, CLR_NORMAL, sizeof(color));
            strncpy(alarm, "LIVE", sizeof(alarm));
            break;
        case SLOG_INFO:
            strncpy(color, CLR_GREEN, sizeof(color));
            strncpy(alarm, "INFO", sizeof(alarm));
            break;
        case SLOG_WARN:
            strncpy(color, CLR_YELLOW, sizeof(color));
            strncpy(alarm, "WARN", sizeof(alarm));
            break;
        case SLOG_DEBUG:
            strncpy(color, CLR_BLUE, sizeof(color));
            strncpy(alarm, "DEBUG", sizeof(alarm));
            break;
        case SLOG_ERROR:
            strncpy(color, CLR_RED, sizeof(color));
            strncpy(alarm, "ERROR", sizeof(alarm));
            break;
        case SLOG_FATAL:
            strncpy(color, CLR_RED, sizeof(color));
            strncpy(alarm, "FATAL", sizeof(alarm));
            break;
        case SLOG_PANIC:
            strncpy(color, CLR_RED, sizeof(color));
            strncpy(alarm, "PANIC", sizeof(alarm));
            break;
        case SLOG_TIME:
            strncpy(color, CLR_GREEN, sizeof(color));
            strncpy(alarm, "TIME", sizeof(alarm));
            slg.to_console = 0;
            break;
        case SLOG_FULL:
            strncpy(color, CLR_BLUE, sizeof(color));
            strncpy(alarm, "FULL", sizeof(alarm));
            break;
        case SLOG_NONE:
            strncpy(prints, string, sizeof(string));
            break;
        default:
            strncpy(prints, string, sizeof(string));
            flag = SLOG_NONE;
            break;
        }

        /* Print output. */
        if (slg.exclusive && flag <= slg.level)
            if (slg.to_console != 0)
                if (flag <= slg.level || slg.pretty)
                {
                    if (flag != SLOG_NONE)
                    {
                        sprintf(prints, "[%s] %s", strclr(color, alarm), string);
                    }
                    if (flag >= slg.level)
                    {
                        printf("%s", slog_get(&mdate, (char *)"%s\n", prints));
                    }
                }

        /* Save log in file. */
        if (slg.to_file && flag <= slg.file_level)
        {
            if (slg.pretty)
            {
                if (flag != SLOG_NONE)
                {
                    sprintf(prints, "[%s] %s", strclr(color, alarm), string);
                }
                // output = slog_get(&mdate, (char *)"%s\n", prints);
            }
            else
            {
                if (flag != SLOG_NONE)
                {
                    sprintf(prints, "[%s] %s", alarm, string);
                }
            }
            output = slog_get(&mdate, (char *)"%s\n", prints);

            /* Add log line to file. */
            slog_to_file(output, slg.fname, &mdate);
        }
    }

    /* Unlock mutex. */
    if (slg.td_safe)
    {
        int rc;
        if ((rc = pthread_mutex_unlock(&slog_mutex)))
        {
            fprintf(stderr, "[ERROR][%s] <%s:%d> inside %s(): Can not deinitialize mutex: %s\n",
                    slg.fname, __FILE__, __LINE__, __func__, strerror(rc));
            exit(EXIT_FAILURE);
        }
    }

    // fprintf(stderr,"prev_errno=%d, actual_errno=%d\t", prev_errno, errno);
    errno = prev_errno;
}

void slog_init(const char *fname, int lvl, int writeFile, int debugConsole, int debugColor, int filestamp, int t_safe, unsigned int rank)
{
    // int status = 0;

    /* Set up default values. */
    slg.level = lvl;           /* Get max log level to print in stdout. */
    slg.file_level = lvl;      /* Level required to write in file. */
    slg.to_file = writeFile;   /* Get max log level to write in file. If 0 will not write to file.*/
    slg.pretty = debugColor;   /* If 1 will output with color. */
    slg.filestamp = filestamp; /* If 1 will add date to log name. */
    slg.to_console = debugConsole;
    slg.td_safe = t_safe;
    slg.fname = fname;
    slg.exclusive = 1; /* If 1 will exclude other levels different to the chose one */
    slg.rank = rank;   /* Identifier used when multiple process are writing over the same file */

    /* Init mutex sync. */
    if (t_safe)
    {
        /* Init mutex attribute. */
        pthread_mutexattr_t m_attr;
        int rc;
        if ((rc = pthread_mutexattr_init(&m_attr)) ||
            (rc = pthread_mutexattr_settype(&m_attr, PTHREAD_MUTEX_RECURSIVE)) ||
            (rc = pthread_mutex_init(&slog_mutex, &m_attr)) ||
            (rc = pthread_mutexattr_destroy(&m_attr)))
        {
            fprintf(stderr, "[ERROR] <%s:%d> inside %s(): Can not initialize mutex: %s\n",
                    __FILE__, __LINE__, __func__, strerror(rc));
            slg.td_safe = 0;
        }
    }

    // /* Parse config file. */
    // if (conf != NULL)
    // {
    //     slg.fname = fname;
    //     status = parse_config(conf);
    // }

    // /* Handle config parser status. */
    // if (!status)
    // {
    //     slog(0, SLOG_INFO, "Initializing logger values without config");
    // }
    // else
    // {
    //     slog(0, SLOG_INFO, "Loading logger config from: %s", conf);
    // }
}

int getLevel(char *str)
{
    // int str_as_num = atoi(str);
    int ret = -1;

    if (!strcmp(str, "SLOG_NONE"))
        ret = SLOG_NONE;
    if (!strcmp(str, "SLOG_LIVE"))
        ret = SLOG_LIVE;
    if (!strcmp(str, "SLOG_DEBUG"))
        ret = SLOG_DEBUG;
    if (!strcmp(str, "SLOG_WARN"))
        ret = SLOG_WARN;
    if (!strcmp(str, "SLOG_INFO"))
        ret = SLOG_INFO;
    if (!strcmp(str, "SLOG_ERROR"))
        ret = SLOG_ERROR;
    if (!strcmp(str, "SLOG_FATAL"))
        ret = SLOG_FATAL;
    if (!strcmp(str, "SLOG_PANIC"))
        ret = SLOG_PANIC;
    if (!strcmp(str, "SLOG_TIME"))
        ret = SLOG_TIME;
    if (!strcmp(str, "SLOG_FULL"))
        ret = SLOG_FULL;

    if (ret == -1)
    {
        fprintf(stderr, "Invalid option, setting SLOG_PANIC as default");
        ret = SLOG_PANIC;
    }

    return ret;
}
