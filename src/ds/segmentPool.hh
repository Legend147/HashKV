#ifndef __SEGMENT_POOL_HH__
#define __SEGMENT_POOL_HH__

#include <vector>
#include <unordered_map>
#include <mutex>
#include "list.hh"
#include "segment.hh"
#include "../configManager.hh"

class SegmentPool {
public:
    SegmentPool();
    SegmentPool(int size, int type);
    ~SegmentPool();

    Segment allocSegment(segment_id_t cid, std::mutex **lock = 0, segment_len_t segmentSize = INVALID_LEN);
    bool releaseSegment(segment_id_t cid);

    inline segment_len_t getPoolSegmentSize() {
        switch (_poolType) {
            case poolType::log:
                return ConfigManager::getInstance().getLogSegmentSize();
            case poolType::main:
            default:
                return ConfigManager::getInstance().getMainSegmentSize();
        }
        return INVALID_LEN;
    }

    enum poolType {
        main,
        log
    };

    static const int defaultSize = 50;          // allocate 50 segment by default
    static const poolType defaultType = poolType::log;

    std::pair<int, int> getUsage();

private:
    struct {
        hlist_head free;
        hlist_head inUsed;
        std::vector<SegmentRec> all;
    } _segments;
    struct {
        int inUsed;
    } _count;
    std::unordered_map<segment_id_t, SegmentRec*> _idToSegment;
    std::mutex _poolLock;

    int _poolType;
    int _poolSize;

    // avoid the pool being copied
    SegmentPool(const SegmentPool&);
    SegmentPool& operator=(const SegmentPool&);

    bool init(int size, int type);
};

#endif
