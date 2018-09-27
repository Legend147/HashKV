#ifndef __GC_MANAGER_HH__
#define __GC_MANAGER_HH__

#include <stdlib.h>
#include <unordered_map>
#include "deviceManager.hh"
#include "keyManager.hh"
#include "segmentGroupManager.hh"
class ValueManager;  // need GCManager for flush (not a good hack ..)
#include "valueManager.hh"

class GCManager {
public:
    GCManager(KeyManager *keyManager, ValueManager *valueManager, DeviceManager *deviceManager, SegmentGroupManager *segmentGroupManager, bool isSlave = false);
    ~GCManager();

    size_t gcAll();
    size_t gcGreedy(bool needGCLock = true, bool needsLockCentralizedReservedPool = true, group_id_t *reportGroupId = 0);
    size_t gcVLog();
    
    void printStats(FILE *out = stdout);

private:
    bool _isSlave;
    bool _useMmap;

    int _maxGC; // no. of max groups involved in a GC operation

    KeyManager *_keyManager;
    ValueManager *_valueManager;
    DeviceManager *_deviceManager;
    SegmentGroupManager *_segmentGroupManager;

    struct {
        Segment read;
        Segment write;
    } _gcSegment;

    size_t _gcWriteBackBytes;
    std::unordered_map<int, len_t> _modeCount;
    struct {
        long ops;
        long groups;
        len_t scanSize;
    } _gcCount;
    
    boost::threadpool::pool _gcReadthreads;

    inline size_t gcOneGroup(group_id_t groupId, GCMode &gcMode, bool needsLockCentralizedReservedPool = true, len_t originBytes = 0, group_id_t *reportGroupId = 0);
    size_t gcSegment(group_id_t mainGroupId, Segment &segment, std::unordered_map<unsigned char *, std::pair<int, ValueLocation>, hashKey, equalKey> &keyCount, len_t total = INVALID_LEN, bool isRemove = false, size_t reservedPos = 0, int gcMode = ALL, size_t *validBytes = 0);
    segment_len_t gcKvPair(group_id_t mainGroupId, Segment *segment, segment_len_t scanned, std::unordered_map<unsigned char *, std::pair<int, ValueLocation>, hashKey, equalKey> &keyCount, bool isRemove = false, size_t reservedPos = 0, int gcMode = ALL, size_t *validBytes = 0);

    inline int getHotness(group_id_t groupId, int updateCount);
    GCMode getGCMode(group_id_t groupId, len_t reservedBytes = 0);

    bool isLogOnly (GCMode gcMode) {
        return (gcMode == LOG_ONLY);
    }

};
#endif /* define __GC_MANAGER_HH__ */
