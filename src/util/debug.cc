#include "debug.hh"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <iostream>
#include <string>
#include <sstream>
#include <unistd.h>

#define DIM(x) (sizeof(x)/sizeof(*(x)))

static const std::string sizes[] = { "EB", "PB", "TB", "GB", "MB", "KB", "B" };
static const uint64_t exbibytes = 1024ULL * 1024ULL * 1024ULL * 1024ULL
        * 1024ULL * 1024ULL;

void printhex(char* buf, int n) {
    int i;
    for (i = 0; i < n; i++) {
        if (i > 0)
            printf(":");
        printf("%02x", buf[i]);
    }
    printf("\n");
}


std::string getTime() {
    struct timeval tv;
    struct tm* ptm;
    long milliseconds;
    char time_string[40];

    gettimeofday(&tv, NULL);
    ptm = localtime(&tv.tv_sec);
    strftime(time_string, sizeof(time_string), "%H:%M:%S", ptm);
    milliseconds = tv.tv_usec / 1000;
    sprintf(time_string, "%s.%03ld", time_string, milliseconds);
    return std::string(time_string);
}

std::string formatSize(uint64_t size) {
    std::string result;
    uint64_t multiplier = exbibytes;
    int i;

    for (i = 0; i < (int) DIM(sizes); i++, multiplier /= 1024) {
        if (size < multiplier) {
            continue;
        }
        if (size % multiplier == 0) {
            result = std::to_string(size / multiplier) + sizes[i];
        } else {
            std::stringstream ss;
            ss.precision(2);
            ss << std::fixed << (float) size / multiplier << sizes[i];
            result = ss.str();
        }
        return result;
    }
    result = "0";
    return result;
}

void dumpBuf (const char* path, char* buf, int n) {
    FILE* f = fopen (path, "w");
    fwrite(buf, 1, n, f);
    fclose(f);
}

void fillRandom (char* buf, int n) {
    int fd = open("/dev/urandom", O_RDONLY);
    if (read(fd, buf, n) < 0) {
        perror ("read from random");
        exit(-1);
    }
    close(fd);
}
