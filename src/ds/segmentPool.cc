#include "segmentPool.hh"

SegmentPool::SegmentPool() {
    init(SegmentPool::defaultSize, SegmentPool::defaultType);
}

SegmentPool::SegmentPool(int size, int type) {
    init(size, type);
}

SegmentPool::~SegmentPool() {
    for (auto rec : _segments.all) {
        Segment::free(rec.segment);
        delete rec.lock;
    }
}

Segment SegmentPool::allocSegment(segment_id_t cid, std::mutex **lock, segment_len_t segmentSize) {
    Segment segment;

    if (segmentSize == INVALID_LEN) segmentSize = getPoolSegmentSize();

    std::lock_guard<std::mutex> poolLock(_poolLock);

    // refuse to alloc segment for duplicated id
    if (_idToSegment.count(cid) > 0) {
        if (Segment::getSize(_idToSegment.at(cid)->segment) != segmentSize) {
            if (Segment::init(_idToSegment.at(cid)->segment, cid, segmentSize, false) == false) {
                debug_error("Failed to init segment %lu of size %lu\n", cid, segmentSize);
            }
        }
        return _idToSegment.at(cid)->segment;
    }

    // return invalid segment if no free segments available
    if (hlist_empty(&_segments.free)) {
        debug_error("POOL is out of segment for segment: %lu!!!\n", cid);
        assert(0);
        return segment;
    }

    // move the rec from free to inUse, set the mapping between segment id and segment rec
    SegmentRec *rec = segment_of(_segments.free.first, SegmentRec, hnode);
    hlist_del(&rec->hnode);
    hlist_add_head(&rec->hnode, &_segments.inUsed);
    _idToSegment[cid] = rec;
    Segment::assignId(rec->segment, cid);

    // resize the segment if necessary
    if (segmentSize != getPoolSegmentSize()) {
        if (Segment::init(rec->segment, cid, segmentSize, false) == false) {
            debug_error("Failed to init segment %lu of size %lu\n", cid, segmentSize);
        }
    }

    // prepare the return values
    segment = rec->segment;

    if (lock != 0) {
        *lock = rec->lock;
    }

    _count.inUsed++;

    return segment;
}

bool SegmentPool::releaseSegment(segment_id_t cid) {
    std::lock_guard<std::mutex> poolLock(_poolLock);
    // return false if segment is not in the pool, i.e., cannot release
    if (_idToSegment.count(cid) == 0)
        return false;

    // move from inUse to free
    std::unordered_map<segment_id_t, SegmentRec*>::iterator rec = _idToSegment.find(cid);
    assert(cid == Segment::getId(rec->second->segment));
    hlist_del(&rec->second->hnode);
    hlist_add_head(&rec->second->hnode, &_segments.free);
    Segment::assignId(rec->second->segment, 0);

    // guarantee clean segment upon reallocated (?)
    //Segment::clean(rec->second->segment);
    Segment::resetFronts(rec->second->segment);
    
    // resize the segment if necessary
    if (Segment::getSize(rec->second->segment) != getPoolSegmentSize()) {
        Segment::init(rec->second->segment, cid, getPoolSegmentSize(), false);
    }
    _idToSegment.erase(cid);
    debug_info("Release %lu to pool\n", cid);
    _count.inUsed--;

    return true;
}

bool SegmentPool::init(int size, int type) {
    std::lock_guard<std::mutex> poolLock(_poolLock);

    // check if the type of pool is supported
    switch (type) {
        case poolType::log: // reserved
            break;
        case poolType::main: // data
            break;
        default:
            printf("Unknown type for SegmentPool!\n");
            return false;
    }

    _poolType = type;
    _poolSize = size;

    // init lists keeping track of segments
    INIT_HLIST_HEAD(&_segments.free);
    INIT_HLIST_HEAD(&_segments.inUsed);

    // allocate space for records and segments
    _segments.all.resize(size);
    for (int i = 0; i < size; i++) {
        INIT_HLIST_NODE(&_segments.all[i].hnode);
        Segment::init(
            _segments.all[i].segment, 0, 
            getPoolSegmentSize(),
            false
        );
        hlist_add_head(&_segments.all[i].hnode, &_segments.free);
        _segments.all[i].lock = new std::mutex();
    }
    _count.inUsed = 0;

    return true;
}

std::pair<int, int> SegmentPool::getUsage() {
    // free, inUsed
    return std::pair<int, int>(_poolSize - _count.inUsed, _count.inUsed);
}
