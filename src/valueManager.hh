#ifndef __VALUE_MANAGER_HH__
#define __VALUE_MANAGER_HH__

#include <unordered_set>
#include <unordered_map>
#include <queue>
#include <vector>
#include <pthread.h>
#include "statsRecorder.hh"
#include "ds/segment.hh"
#include "ds/segmentPool.hh"
#include "ds/keyvalue.hh"
#include "ds/list.hh"
#include "define.hh"
#include "deviceManager.hh"
#include "segmentGroupManager.hh"
#include "keyManager.hh"
#include "logManager.hh"
class GCManager;
#include "gcManager.hh"

class ValueManager {
    friend class GCManager;
public:
    ValueManager(DeviceManager *deviceManager, SegmentGroupManager *segmentGroupManager, KeyManager *keyManager, LogManager *logManager = 0, bool isSlave = false);
    ~ValueManager();

    bool getValueFromBuffer (const char *keyStr, char *&valueStr, len_t &valueSize);
    bool getValueFromDisk (const char *keyStr, ValueLocation valueLoc, char *&valueStr, len_t &valueSize);

    ValueLocation putValue (char *keyStr, len_t keySize, char *valueStr, len_t valueSize, const ValueLocation &oldValueLoc, int hotness = 1);

    bool forceSync();


    /**
     * prepareGCGroupInCentralizedPool
     *
     * \parm groupId the group to GC
     * \pram needsLock whether to lock the group metadata
     *
     */
    bool prepareGCGroupInCentralizedPool(group_id_t groupId, bool needsLock = true);
    /**
     * cleanupGCGroupInCentralizedPool
     *
     * \parm groupId the group to GC
     * \pramgcIsDone whether GC is successful 
     *
     */
    bool cleanupGCGroupInCentralizedPool(group_id_t groupId, bool gcIsDone = true, bool needsLockPool = true);

    void printSlaveStats(FILE *out = stdout);

    bool setGCManager(GCManager *gcManager) {
        return ((_gcManager = gcManager) != 0);
    }

    static int spare; // level of spare group buffer for flushing reserved space

protected:
    std::mutex _GCLock;

private:
    DeviceManager *_deviceManager; // deviceManager
    SegmentGroupManager *_segmentGroupManager; // groupMetaDataManager
    KeyManager *_keyManager; // keyManager, for flush of centralized buffer (Todo avoid dependency)
    GCManager *_gcManager; // gcManager, for GC during flush of centralized buffer (not a good hack ..)
    LogManager *_logManager; // logManager, for logging before metadata updates
    ValueManager *_slaveValueManager; // slave value manager

    struct {
        DeviceManager *dm;
        SegmentGroupManager *cgm;
        GCManager *gcm;
        len_t writtenBytes;
        len_t validBytes;
    } _slave;

    bool _isSlave; // whether this is the slave value manager

    std::vector<list_head> _activeSegments; // levels of active buffers
    std::vector<SegmentBuffer*> _curSegmentBuffer; // write frontier at each buffer level
    std::vector<SegmentBuffer> _segmentBuffers; // all segments

    std::unordered_set<segment_id_t> _segmentsInBuf; // set of ids of segments in buffer

    std::unordered_map<segment_id_t, SegmentBuffer> _segmentReservedInBuf; // set of ids of segment reserved space in buffer
    std::unordered_map<group_id_t, list_head> _segmentReservedByGroup; // map of groups with segment reserved space in buffer

    SegmentPool *_segmentReservedPool; // pool of segment reserved space
    struct {
        Segment pool;                    // abstract the pool as a segment
        size_t size;                       // size of pool
        std::mutex lock;                   // lock
        // pointers to keys in pool, <segment id, <total update size, <offset in pool> > >
        std::map< segment_id_t, std::pair<segment_len_t, std::set<segment_len_t> > > segmentsInPool; 
        std::unordered_map<unsigned char*, segment_len_t, hashKey, equalKey> keysInPool;
        // <group id, total update size>
        std::unordered_map<group_id_t, std::unordered_multiset<len_t>> groupsInPool;
    } _centralizedReservedPool[MAX_CP_NUM+1];            // centralized reserved space

    struct {
        volatile int flushNext; // pool to flush next
        volatile int inUsed;    // pool in used
        std::queue<std::pair<int, StatsType> > queue; // poolIndex, statsType
        pthread_mutex_t queueLock;
        pthread_cond_t flushedBuffer;
    } _centralizedReservedPoolIndex;


    Segment _readBuffer;                 // read buffer (for single-thread request processing only)

    Segment _zeroSegment;

    boost::threadpool::pool _iothreads;
    boost::threadpool::pool _flushthreads;
    pthread_t _bgflushThread;

    pthread_cond_t _needBgFlush;

    bool _started;

    //coding_parm_t inline findSegmentInGroup(const GroupMetaData *smd, segment_id_t segmentId, device_id_t &deviceId, lba_t &lba);

    //@parm groupMetaOutDate indicate the flush fronts in group metadata are no longer valid (due to batch metadata update)
    bool setGroupReservedBufferCP(group_id_t groupId, bool needsLock, bool isGC, group_id_t reservedGroupId = INVALID_GROUP, bool groupMetaOutDated = false, std::unordered_map<std::pair<segment_id_t, segment_id_t>, len_t, hashCidPair> *invalidBytes = 0, int poolIndex = 0);
    bool releaseGroupReservedBufferCP(group_id_t groupId, bool needsLockPool, bool isGC, bool isGCdone = true, int poolIndex = 0);

    bool outOfReservedSpace(offset_t flushFront, group_id_t groupId, int poolIndex);
    bool outOfReservedSpaceForObject(offset_t flushFront, len_t objectSize);

    void flushCentralizedReservedPool(group_id_t *reportGroupId = 0, bool isUpdate = false, int poolIndex = 0, std::unordered_map<unsigned char*, offset_t, hashKey, equalKey> *oldLocations = 0); 
        // <group id, total update size>
    void flushCentralizedReservedPoolBg(StatsType stats);
    static void* flushCentralizedReservedPoolBgWorker(void *arg);
    int getNextPoolIndex(int current);
    void decrementPoolIndex(int &current);

    void flushCentralizedReservedPoolVLog(int poolIndex = 0);
    void flushVLogCentralizedReservedPool();

    std::pair<offset_t, len_t> flushSegmentToWriteFront(Segment &segment, bool isGC = false);

    offset_t getLastSegmentFront(offset_t flushFront);

    void logMetaPersist(std::set<segment_id_t> &modifiedSegments, std::set<group_id_t> &modifiedGroups);

    void restoreFromUpdateLog();
    void restoreFromGCLog();

};
#endif /* __VALUE_MANAGER_HH__ */
