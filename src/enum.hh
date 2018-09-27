#ifndef __ENUM_HH__
#define __ENUM_HH__

#include <unistd.h>

enum CodingScheme {
    RAID0,
    REPLICATION,
    RAID5,
    RDP,
    EVENODD,
    CAUCHY,
    DEFAULT
};

enum DataType {
    KEY,
    VALUE,
    META
};

enum DiskType {
    DATA,
    LOG,
    MIXED
};

enum RequestType {
    READ          = 0x00,
    WRITE         = 0x01,
    FLUSH         = 0x10,
    COMMIT        = 0x20,
    WIRTE_KEY     = 0x03,
    READ_VALUE    = 0x04,
    WIRTE_VALUE   = 0x05,
};

enum class DebugLevel: int {
    NONE,
    ERROR,
    WARN,
    INFO,
    TRACE,
    ANY
};

enum DBType {
    LEVEL         = 0x00,
};

enum GCMode {
    ALL,                   // 0
    LOG_ONLY,              // 1
};

#endif /* ENUM_HH_ */
