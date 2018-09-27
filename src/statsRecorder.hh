#ifndef __STATS_RECORDER_HH__
#define __STATS_RECORDER_HH__
#include <stdio.h>
#include <sys/time.h>
#include <climits>
#include <unordered_set>
#include <unordered_map>
#include <map>
#include <vector>

#include <hdr_histogram.h>
#include "define.hh"

#define S2US (1000 * 1000)
#define MAX_DISK (2)


enum StatsType {
    KEY_IDENTIFY,
    /* Set */
    SET,
    SET_KEY_LOOKUP,
    SET_KEY_WRITE,
    SET_KEY_WRITE_SHADOW,
    SET_VALUE,
    /* Update */
    UPDATE,
    UPDATE_KEY_LOOKUP,
    UPDATE_KEY_LOOKUP_LSM,
    UPDATE_KEY_LOOKUP_CACHE,
    UPDATE_KEY_WRITE,
    UPDATE_KEY_WRITE_LSM,
    UPDATE_KEY_WRITE_LSM_GC,
    UPDATE_KEY_WRITE_CACHE,
    UPDATE_KEY_WRITE_SHADOW,
    UPDATE_VALUE,
    /* Read */
    GET,
    GET_KEY_LOOKUP,
    GET_VALUE,
    /* Scan */
    SCAN,
    /* GC */
    GC_WRITE_BYTES,
    GC_SCAN_BYTES,
    GC_READ,
    GC_PRE_FLUSH,
    GC_FLUSH,
    GC_IN_FLUSH,
    GC_IN_FLUSH_WITH_SYNC,
    GC_KEY_LOOKUP,
    GC_OTHERS,
    GC_TOTAL,
    GC_UPDATE_COUNT,
    /* LSM / Key processing */
    KEY_GET_ALL,
    KEY_GET_CACHE,
    KEY_GET_LSM,
    KEY_GET_SHADOW,
    KEY_SET_ALL,
    KEY_SET_CACHE,
    KEY_SET_LSM,
    KEY_SET_LSM_BATCH,
    KEY_SET_SHADOW,
    KEY_UPDATE_CACHE,
    /* FLUSH */
    GROUP_IN_POOL_FLUSH,
    GROUP_OTHER_FLUSH,
    POOL_FLUSH,
    FLUSH_BYTES,
    /* Metadata consistency */
    LOG_TIME,
    /* Device */
    DATA_WRITE_BYTES,
    FLUSH_SYNC,
    /* Others */
    UPDATE_TO_MAIN,
    UPDATE_TO_LOG,
    GC_RATIO_UPDATE,
    GC_INVALID_BYTES_UPDATE,
    /*
    IO,
    FLUSH2,
    WRITE_KEY_LSM,
    SYNC,
    UPDATEPROCESS,
    SETWRITEKEY,
    RESTRIPERESERVED,
    ENCODE,
    FLUSH_SYNC,
    FLUSHZERO,
    UPDATEKEYONESTRIPE,
    ALLSETTIME,
    ALLUPDATE,
    ALLGC,
    COLD,
    HOT,
    FLUSHUPDATEKEY,
    GETKEYSHADOW,
    GETKEYCACHE,
    GETKEYLSM,
    GETINUNFIEDBUFFER,
    GETINRESERVEDBUFFER,
    GETINNEWWRITEBUFFER,
    GETINDISK,
    GET,
    GETLOOKUPTIME,
    GETINIT,
    GETDATA,
    READDATA,
    GETFREE,
    FLUSHBYTES,
    BLOCKBYTES,
    UPDATEVALUETIME,
    GETKEYALL,
    GCINVALIDLOG,
    GCINVALIDMAIN,
    GCVALIDLOG,
    GCVALIDMAIN,
    GC_SCAN_BYTES,
    */
    NUMLENGTH
};

class StatsRecorder {
 
public:
    static unsigned long long timeAddto(struct timeval &start_time,unsigned long long &resTime);

    static StatsRecorder *getInstance();
    static void DestroyInstance();

#define STAT_TIME_PROCESS(_FUNC_, _TYPE_) \
    do { \
        struct timeval startTime; \
        gettimeofday(&startTime, 0); \
        _FUNC_; \
        StatsRecorder::getInstance()->timeProcess(_TYPE_, startTime); \
    } while(0);

#define STAT_TIME_PROCESS_VS(_FUNC_, _TYPE_, _VS_) \
    do { \
        struct timeval startTime; \
        gettimeofday(&startTime, 0); \
        _FUNC_; \
        StatsRecorder::getInstance()->timeProcess(_TYPE_, startTime, 0, 1, _VS_); \
    } while(0);

    bool inline IsGCStart(){
        return startGC;
    }

    void totalProcess(StatsType stat, size_t diff, size_t count = 1);
    unsigned long long timeProcess(StatsType stat, struct timeval &start_time, size_t diff = 0, size_t count = 1, unsigned long long valueSize = 0);

    void inline IOBytesWrite(unsigned int bytes,unsigned int diskId){
        if (!statisticsOpen) return;
        IOBytes[diskId].first += bytes;
    }

    void inline IOBytesRead(unsigned int bytes,unsigned int diskId){
        if (!statisticsOpen) return;
        IOBytes[diskId].second += bytes;
    }

    void minMaxGCUpdate(unsigned int mn, unsigned int mx) {
        if (!statisticsOpen) return;
        if (mn < min[GC_UPDATE_COUNT]) min[GC_UPDATE_COUNT] = mn;
        if (mx > max[GC_UPDATE_COUNT]) max[GC_UPDATE_COUNT] = mx;
    }

    void openStatistics(struct timeval &start_time);
    void printProcess(const char* arg1,unsigned int i);


    void putGCGroupStats(unsigned long long validMain, unsigned long long validLog, unsigned long long invalidMain, unsigned long long invalidLog, unsigned long long validLastLog);
    void putFlushGroupStats(unsigned long long validDataGroups, std::unordered_map<group_id_t, unsigned long long> &counts);

private:
    StatsRecorder(); 
    ~StatsRecorder();
  
    static StatsRecorder* mInstance;

    bool statisticsOpen;
    bool startGC;
    unsigned long long time[NUMLENGTH];
    unsigned long long total[NUMLENGTH];
    unsigned long long max[NUMLENGTH];
    unsigned long long min[NUMLENGTH];
    unsigned long long counts[NUMLENGTH];
    unsigned long long gcNums;
    unsigned long long gcInMemNums;
    unsigned long long us;
    unsigned long long lsmLookupTime;
    unsigned long long approximateMemoryUsage;
    std::vector<std::pair<unsigned long long, unsigned long long>> IOBytes; /* write,read */

    struct hdr_histogram *_updateTimeHistogram;
    struct hdr_histogram *_getTimeHistogram;
    std::map<unsigned long long, struct hdr_histogram*> _getByValueSizeHistogram;
    std::map<unsigned long long, struct hdr_histogram*> _updateByValueSizeHistogram;


    struct Stats {
        unsigned long long *buckets[2];
        unsigned long long sum[2];
        unsigned long long count[2];
    };

    struct {
        struct Stats valid;
        struct Stats invalid;
        struct Stats validLastLog;
        unsigned int bucketLen;
        unsigned long long bucketSize;
    } gcGroupBytesCount;

    struct Stats flushGroupCount;
    int flushGroupCountBucketLen;

    enum {
        MAIN,
        LOG
    };
};

#endif

