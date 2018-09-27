#ifndef __SEGMENT_GROUP_MANAGER_HH__
#define __SEGMENT_GROUP_MANAGER_HH__

#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include "ds/bitmap.hh"
#include "ds/minheap.hh"
#include "ds/maxheap.hh"
#include "ds/keyvalue.hh"
class SegmentGroupManager;
class GCManager;
#include "gcManager.hh"
#include "keyManager.hh"

class SegmentGroupManager {
    friend class GCManager;

public:
    SegmentGroupManager(bool isSlave = false, KeyManager *keyManager = 0);
    ~SegmentGroupManager();

    // group create
    bool getNewMainSegment(group_id_t &groupId, segment_id_t &segmentId, bool needsLock);
    // group destory
    bool freeGroup(group_id_t groupId, bool releaseLock);
    // group expansion
    bool getNewLogSegment(group_id_t &groupId, segment_id_t &segmentId, bool needsLock);
    // group shrink
    bool freeSegment(segment_id_t segmentId, bool releaseLock);
    void releaseGroupLogSegments(group_id_t groupId, bool needsLock);

    // group frontiters
    bool setSegmentFlushFront(segment_id_t segmentId, segment_len_t flushFront);
    segment_len_t getSegmentFlushFront(segment_id_t segmentId);
    offset_t getGroupWriteFront(group_id_t groupId, bool needsLock);
    offset_t getGroupFlushFront(group_id_t groupId, bool needsLock);
    offset_t setGroupWriteFront(group_id_t groupId, offset_t front, bool needsLock);
    offset_t setGroupFlushFront(group_id_t groupId, offset_t front, bool needsLock);
    offset_t resetGroupFronts(group_id_t groupId, bool needsLock);

    // group lock
    bool getGroupLock(group_id_t groupId);
    bool releaseGroupLock(group_id_t groupId);
    // group info (main)
    len_t addGroupMainSegmentBytes(group_id_t groupId, len_t diff, bool needsLock = false);
    len_t removeGroupMainSegmentBytes(group_id_t groupId, len_t diff, bool needsLock = false);
    len_t getGroupMainSegmentBytes(group_id_t groupId, bool needsLock = false);
    len_t getGroupMainSegmentMaxBytes(group_id_t groupId, bool needsLock = false);
    void resetGroupByteCounts(group_id_t groupId, bool needsLock);
    len_t releaseData(group_id_t groupId, len_t bytes);
    // group info (log)
    len_t useReserved(group_id_t groupId, len_t bytes);
    // group / segment retrieval
    group_id_t getGroupBySegmentId(segment_id_t segmentId);
    segment_id_t getMainSegmentBySegmentId(segment_id_t segmentId);
    segment_id_t getGroupMainSegment(group_id_t groupId);
    bool convertRefMainSegment(ValueLocation &valueLoc, bool needsLock = false);
    bool hasLogSegments(group_id_t groupId, bool needsLock);
    std::vector<segment_id_t> getGroupSegments(group_id_t groupId, bool needsLock);
    std::vector<segment_id_t> getGroupLogSegments(group_id_t groupId, bool needsLock);
    double getGroupWriteBackRatio (group_id_t groupId, int type, bool isGC = false);
    // group / segment set
    /* hack only for value manager to restore consistency ... */
    bool setGroupSegments(group_id_t groupId, std::vector<segment_id_t> segments);
    // segment operations
    //bool getSegmentLock(segment_id_t segmentId);
    //bool releaseSegmentLock(segment_id_t segmentId);
    
    // vlog
    offset_t getAndIncrementVLogWriteOffset(len_t diff, bool isGC = false);
    offset_t getAndIncrementVLogGCOffset(len_t diff);

    len_t getLogWriteOffset();
    len_t getLogGCOffset();
    len_t getLogValidBytes();
    len_t getLogWrittenBytes();

    // print out
    void printUsage(FILE *out = stdout);
    void printGroups(FILE *out = stdout);
    double getCurrentUsage();
    // overall usage info
    segment_id_t getNumFreeMainSegments();
    segment_id_t getNumFreeLogSegments();
    segment_id_t getNumFreeSegments();

    // checking
    bool isLogSegment(segment_id_t segmentId);

    // metadata keys and values
    static std::string getSegmentKey(segment_id_t segmentId);
    static std::string getGroupKey(group_id_t groupId);
    static std::string generateSegmentValue(offset_t writeFront);
    static std::string generateGroupValue(offset_t writeFront, std::vector<segment_id_t> segments);
    bool writeSegmentMeta(segment_id_t segmentId);
    bool writeGroupMeta(group_id_t groupId);

    static bool readGroupMeta(const std::string &metadata, offset_t &writeFront, std::vector<segment_id_t> &segments);

    // restore
    bool restoreMetaFromDB();

    static const char *LogHeadString;
    static const char *LogTailString;
    static const char *SegmentPrefix;
    static const char *GroupPrefix;
    static const char *ListSeparator;
    static const char *LogValidByteString;
    static const char *LogWrittenByteString;

private:
    bool _isSlave;

    typedef struct {
        std::vector<segment_id_t> segments;
        std::unordered_map<segment_id_t, int> position; // also flushfront of segment
        offset_t writeFront;
        offset_t flushFront;
        len_t mainSegmentValidBytes;
        len_t maxMainSegmentBytes;
    } GroupMetaData;

    segment_id_t _MaxSegment;
    segment_id_t _freeLogSegment;
    segment_id_t _freeMainSegment;

    struct {
        BitMap *group;
        BitMap *segment;
    } _bitmap;

    struct {
        segment_id_t segment;
    } _writeFront;

    struct {
        offset_t writeFront;
        offset_t gcFront;
    } _vlog;

    KeyManager *_keyManager;

    struct {
        std::unordered_map<segment_id_t, std::pair<group_id_t, bool> > segment;  // segmentId -> (groupId, locked)
        std::unordered_map<segment_id_t, segment_len_t> segmentFront;  // segmentId -> flush front
        std::unordered_map<group_id_t, std::pair<GroupMetaData, bool> > group;
        std::mutex lock;
    } _metaMap;

    MaxHeap<len_t> *_maxSpaceToRelease;                     // max amount of space to release, including 
                                                            // (1) invalid data in data area, 
                                                            // (2) max space used in reserved space, or 
                                                            // (3) max space used as reserved segment
    MinHeap<double> *_minWriteBackRatio;                    // min amount to write back

    std::unordered_set<segment_id_t> _groupInHeap;                // avoid duplicated GC when stripe is inside the heap but already GC manually

    bool getNewSegment(group_id_t &groupId, segment_id_t &segmentId, bool isLog, bool needsLock);

    len_t changeBytes(group_id_t group_id_t, len_t bytes, int type);
    void updateGroupWriteBackRatio(group_id_t groupId);

    bool accessGroupLock(group_id_t groupId, bool isLock);

    segment_id_t getNumFreeSegments(int type);

    offset_t getAndIncrementVLogOffset(len_t diff, int type, bool isGC = false);

};
#endif
