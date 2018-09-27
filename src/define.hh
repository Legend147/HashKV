#ifndef __DEFINE_HH__
#define __DEFINE_HH__

#include <set>
#include <vector>
#include <utility>
#include <assert.h>          // assert()
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdint.h>          // [u]intXX_t
#include <unistd.h>
#include <boost/thread/shared_mutex.hpp>
#include "enum.hh"

// diskManager
//#define DISK_DIRECT_IO  
//#define PAGE_ALIGN
#define DISK_BLKSIZE    (512)
#define DIRECT_LBA_SEGMENT_MAPPING    1

// stripeMetaDataMod, valueMod
#define MIN_FREE_SEGMENTS (2)

/** align the buffer with block size in memory for direct I/O **/
static inline void* buf_malloc (size_t s) {
    void *buf = 0;
#if defined(DISK_DIRECT_IO) || defined(PAGE_ALIGN)
#ifdef DISK_DIRECT_IO
    int ret = posix_memalign(&buf, DISK_BLKSIZE, s);
#else 
    int ret = posix_memalign(&buf, getpagesize(), s);
#endif
    if (ret != 0) {
        fprintf(stderr, "Failed to allocate buffer with size %lu!!\n", s);
        assert(0);
        exit(-1);
    }
#else
    buf = malloc(s);
#endif
    return buf;
}

static inline void* buf_calloc (size_t s, unsigned int unit) {
    void* ret = buf_malloc(s * unit);
    if (ret != nullptr) memset(ret, 0, s * unit);
    return ret;
}

// common/ds/lru.cc
#define DEFAULT_LRU_SIZE    (500)

// common/debug.hh
#ifndef DEBUG
#define DEBUG    1
#endif

// keyManager (in general)
//#define KEY_SIZE    (16)
#define KEY_SIZE    (24)

// valueManager
#define MAX_CP_NUM  (128)

// all typedef go here
typedef int64_t                                       LL;
typedef uint64_t                                      ULL;
typedef int32_t                                       disk_id_t;
typedef ULL                                           offset_t;
typedef ULL                                           len_t;
typedef ULL                                           segment_id_t;
typedef uint32_t                                      lba_t;
typedef offset_t                                      segment_offset_t;
typedef len_t                                         segment_len_t;
typedef segment_id_t                                group_id_t;
typedef std::pair<offset_t, len_t>                    off_len_t;
typedef off_len_t                                     segment_off_len_t;
typedef std::pair<segment_id_t, std::pair<disk_id_t, offset_t> > StripeLocation; // segment id, offset_t
/* boost shared_mutex, can changed to std::shared_mutex if c++14 available */
typedef boost::shared_mutex RWMutex;


/** DRY_RUN mode w/o disk I/O, or run w/ disk I/O **/
#ifdef DRY_RUN
    #undef ACTUAL_DISK_IO
    #define DISKLBA_OUT
    /** disk block size, used for printing LBAs **/
    #define DISK_BLOCK_SIZE    (4096)
#elif defined DRY_RUN_PERF
    #undef ACTUAL_DISK_IO
#else
    #define ACTUAL_DISK_IO
    #undef DISKLBA_OUT
#endif
//#define READ_AHEAD    (64 * 1024)
#define INVALID_VALUE       (-1)
#define INVALID_LBA         (lba_t) (INVALID_VALUE)
#define INVALID_DISK        (disk_id_t) (INVALID_VALUE)
#define INVALID_OFFSET      (offset_t) (INVALID_VALUE)
#define INVALID_SEGMENT   (segment_id_t) (INVALID_VALUE)
#define INVALID_GROUP       (group_id_t) (INVALID_VALUE)
#define INVALID_LEN         (len_t) (INVALID_VALUE)

#define LSM_SEGMENT       (segment_id_t) (INVALID_VALUE-1)
#define LSM_GROUP           (group_id_t) (INVALID_VALUE-1)

/** default no. of threads, single thread **/
#define NUM_THREAD    1

struct hashCidPair {
    size_t operator() (std::pair<segment_id_t, segment_id_t> const &s) const {
        return std::hash<segment_id_t>{}(s.first) + std::hash<segment_id_t>{}(s.second);
    }
};  

#endif
