#ifndef __CONFIG_MANAGER_HH__
#define __CONFIG_MANAGER_HH__

#include <stdint.h>
#include <string>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include "define.hh"

class ConfigManager {
public:

    static ConfigManager& getInstance() {
        static ConfigManager instance; // Guaranteed to be destroyed
        // Instantiated on first use
        return instance;
    }

    void setConfigPath (const char* key);

    // segments and blocks
    segment_len_t getSegmentSize(bool isLog) const;
    segment_len_t getMainSegmentSize() const;
    segment_len_t getLogSegmentSize() const;
    segment_len_t getNumMainSegment() const;
    segment_len_t getNumLogSegment() const;
    segment_len_t getNumSegment() const;
    len_t getSystemEffectiveCapacity() const;
    int getRetryMax() const;
    bool segmentAsFile() const;
    bool segmentAsSeparateFile() const;

    // buffering 
    bool isUpdateKVBufferEnabled() const;
    segment_len_t getUpdateKVBufferSize() const;
    bool isInPlaceUpdate() const;
    int getNumPipelinedBuffer() const;
    bool usePipelinedBuffer() const;

    // hotness
    int getHotnessLevel() const;
    bool useSlave() const;
    len_t getColdStorageCapacity() const;
    segment_len_t getColdStorageBufferSize() const;
    std::string getColdStorageDevice() const;
    bool useSeparateColdStorageDevice() const;

    // key management
    std::string getLSMTreeDir() const;
    segment_len_t getKVLocationCacheSize() const;
    bool dbNoCompress() const;

    // log metadata
    bool persistLogMeta() const;

    // gc
    uint32_t getGreedyGCSize() const;
    GCMode getGCMode() const;
    uint32_t getNumGCReadThread() const;

    // kv-separation
    uint32_t getMinValueSizeToLog() const;
    bool disableKvSeparation() const;

    // vlog
    bool enabledVLogMode() const;
    uint32_t getVLogGCSize() const;

    // consistency
    bool enableCrashConsistency() const;

    // misc
    uint32_t getHashTableDefaultSize() const;
    int getHashTableDefaultHashMethod() const;
    uint32_t getNumParallelFlush() const;
    uint32_t getNumIOThread() const;
    uint32_t getNumCPUThread() const;
    bool syncAfterWrite() const;
    uint32_t getNumRangeScanThread() const;
    bool enabledScanReadAhead() const;
    len_t getBatchWriteThreshold() const;
    bool useMmap() const;
    int getMaxOpenFiles() const;

    // debug
    DebugLevel getDebugLevel() const;

    void printConfig() const;

private:
    ConfigManager() {}
    ConfigManager(ConfigManager const&); // Don't Implement
    void operator=(ConfigManager const&); // Don't implement
    
    bool readBool (const char* key);
    int readInt (const char* key);
    unsigned int readUInt (const char* key);
    LL readLL (const char* key);
    ULL readULL (const char* key);
    double readFloat(const char* key);
    std::string readString (const char* key);

    boost::property_tree::ptree _pt;

    struct {
        segment_len_t mainSegmentSize;        // no. of blocks per main segment
        segment_len_t logSegmentSize;         // no. of blocks per log segment
        segment_len_t numMainSegment;         // no. of main segments 
        segment_len_t numLogSegment;          // no. of log segments 
        int retryMax;                             // max. no. of retries
        bool segmentAsFile;                     // whether to store segments as file(s)
        bool segmentAsSeparateFile;             // whether to store each segment as a separate file
    } _basic;

    struct {
        segment_len_t updateKVBufferSize;       // size of buffer for updated key-value pairs
        bool inPlaceUpdate;                       // whether to enable in-place update for updated values of the same size
        int numPipelinedBuffer;                   // no. of pipelined update buffers
    } _buffer;

    struct {
        int levels;                               // total number of hotness levels
        bool useSlave;                            // use a slave storage for cold items
        len_t coldStorageSize;                    // size of the cold storage
        std::string coldStorageDevice;            // separate device for cold storage
    } _hotness;

    struct {
        std::string lsmTreeDir;                   // directory for placing LSM-tree
        segment_len_t locationCacheSize;        // max. number of key-value locations to cache
        int dbType;                               // type of db to use
        bool compress;                            // Whether to use snappy compression
    } _key;

    struct {
        bool persist;                             // whether to store log metadata persistently to kv-store
    } _logmeta;

    struct {
        uint32_t greedyGCSize;                    // max. number of segments selected for GC
        GCMode mode;                              // GC mode
        uint32_t numReadThread;                   // Number of read threads to get segments from disk
    } _gc;

    struct {
        uint32_t minValueSizeToLog;               // minimum value size to trigger key-value separation
        bool disabled;                            // whether kv-separation should be disabled
    } _kvsep;

    struct {
        bool enabled;
        uint32_t gcSize;
    } _vlog;

    struct {
        bool crash;
    } _consistency;

    struct {
        uint32_t hashTableDefaultSize;            // default total number of free slots in a hash table
        int hashMethod;                           // hashing function to use
        uint32_t numParallelFlush;                // number of segments to flush in parallel
        uint32_t numIoThread;                     // max. number of I/O threads to use
        uint32_t numCPUThread;                    // max. number of CPU threads available
        bool syncAfterWrite;                      // call fsycn() / sync() for all data after write
        uint32_t numRangeScanThread;              // number of threads for range scan
        bool scanReadAhead;
        len_t batchWriteThreshold;                // max size of batches of writes to a segment for buffer flush
        bool useMmap;
        int maxOpenFiles;                         // max number of open files
    } _misc;

    struct {
        DebugLevel level;                         // level of debug
    } _debug;

};

#endif /* CONFIGMOD_HH_ */
