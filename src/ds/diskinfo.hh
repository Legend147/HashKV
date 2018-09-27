#ifndef __DISKINFO_HH__
#define __DISKINFO_HH__

#include <assert.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include "../define.hh"
#include "../configManager.hh"

struct DiskInfo {
    disk_id_t diskId;                   // disk id
    char diskPath[128];                 // disk path
    offset_t capacity, remaining;       // total capacity, and unused capacity
    bool isLog;                         // log disk
    bool dirty;                         // whether data is written since last sync
    bool alive;                         // whether the disk is alive

    int fd;                             // file descriptor for the disk

    offset_t skipOffset;                // offset to skip from the starting point of the disk

    DiskInfo() {
        diskId = INVALID_DISK;
        strcpy(diskPath, "");
        capacity = 0;
        remaining = 0;
        fd = 0;
        dirty = false;
        alive = true;
        skipOffset = 0;
    };

    DiskInfo(disk_id_t diskId, const char* diskPath, ULL capacity, bool isLogDisk = false, offset_t skip = 0) {
        this->diskId = diskId;
        strncpy(this->diskPath, diskPath, 127);
        this->capacity = capacity;

        // Todo: For benchmark. Assume disk is brand new when starting
        this->remaining = capacity - skip;

        this->skipOffset = skip;

        isLog = isLogDisk;
        alive = true;

        int flag = O_RDWR;
#ifdef DISK_DIRECT_IO
        flag |= O_DIRECT;
#endif
#ifdef DISK_DSYNC
        flag |= O_DSYNC;
#endif

        bool useFS = ConfigManager::getInstance().segmentAsFile();
        if (!useFS) {
            fd = open(diskPath, flag);
            if (fd < 2) {
                perror(strerror(errno));
                assert(0);
            }
        }
    }

    void resetDisk() {
        this->remaining = capacity - skipOffset;
    }


    void setDisk(bool status) {
        alive = status;
        if (alive == false) {
            resetDisk();
        }
    }

    void disconnect() {
        close(fd);
        fd = 0;
    }

};

#endif // __DISKINFO_HH__
