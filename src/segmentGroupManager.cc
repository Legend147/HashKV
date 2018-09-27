#include <float.h>
#include "segmentGroupManager.hh"
#include "configManager.hh"
#include "util/debug.hh"
#include "statsRecorder.hh"

const char* SegmentGroupManager::LogHeadString = "L0G_H3AD";
const char* SegmentGroupManager::LogTailString = "L0G_TA1L";
const char* SegmentGroupManager::SegmentPrefix = "HA5HKV_c";
const char* SegmentGroupManager::GroupPrefix = "HA5HKV_g";
const char* SegmentGroupManager::ListSeparator = ";";
const char* SegmentGroupManager::LogValidByteString = "LOG_VAL1D_BYT3";
const char* SegmentGroupManager::LogWrittenByteString = "LOG_WRITE_BYT3";

#define NEXT_SUB_STR() do { \
        spos = metadata.find_first_of(ListSeparator, spos); \
        epos = metadata.find_first_of(ListSeparator, spos+1); \
        if (epos == std::string::npos) \
            epos = metadata.length() + 1; \
    } while (0)

SegmentGroupManager::SegmentGroupManager(bool isSlave, KeyManager *keyManager) {
    _MaxSegment = ConfigManager::getInstance().getNumSegment();
    _freeLogSegment = ConfigManager::getInstance().getNumLogSegment();
    _freeMainSegment = ConfigManager::getInstance().getNumMainSegment();

    _isSlave = isSlave;
    _keyManager = keyManager;

    _bitmap.segment = new BitMap(_MaxSegment);
    _bitmap.group = new BitMap(_MaxSegment);
    _writeFront.segment = 0;

    std::string value = _keyManager->getMeta(LogTailString, strlen(LogTailString));
    _vlog.writeFront = value.empty()? 0 : stoul(value);

    if (_keyManager)
        value = _keyManager->getMeta(LogHeadString, strlen(LogHeadString));
    _vlog.gcFront = value.empty()? 0 : stoul(value);

    _maxSpaceToRelease = new MaxHeap<len_t>(_MaxSegment+1);
    _minWriteBackRatio = new MinHeap<double>(_MaxSegment+1);

    restoreMetaFromDB();
}

SegmentGroupManager::~SegmentGroupManager() {
    delete _bitmap.group;
    delete _bitmap.segment;
    delete _maxSpaceToRelease;
    delete _minWriteBackRatio;
}

bool SegmentGroupManager::getNewMainSegment(group_id_t &groupId, segment_id_t &segmentId, bool needsLock) {
    std::lock_guard<std::mutex> lk (_metaMap.lock);
    return getNewSegment(groupId, segmentId, false, needsLock);
}

bool SegmentGroupManager::getNewLogSegment(group_id_t &groupId, segment_id_t &segmentId, bool needsLock) {
    std::lock_guard<std::mutex> lk (_metaMap.lock);
    return getNewSegment(groupId, segmentId, true, needsLock);
}

bool SegmentGroupManager::getNewSegment(group_id_t &groupId, segment_id_t &segmentId, bool isLog, bool needsLock) {
    segment_len_t numMainSegment = ConfigManager::getInstance().getNumMainSegment();
    bool fixedSegmentId = segmentId != INVALID_SEGMENT;
    if (fixedSegmentId && _metaMap.segment.count(segmentId) > 0) {
        // segment is alreay allocated
        return false;
    } else if (isLog && groupId == INVALID_GROUP) {
        // log segment must have valid group id
        return false;
    } else {
        int retry = 0;
        do {
            retry ++;
            if (!fixedSegmentId) {
                segmentId = _bitmap.segment->getFirstZeroAndFlip((isLog? numMainSegment : 0));
            } else {
                _bitmap.segment->setBit(segmentId);
            }
            //printf("Found free segment %lu for group %lu isLog=%d \n", segmentId, groupId, isLog);
            if ((isLog && !isLogSegment(segmentId)) || (!isLog && isLogSegment(segmentId))) {
                // cannot allocate a segment in designated area
                if (segmentId != INVALID_SEGMENT)
                    _bitmap.segment->clearBit(segmentId);
                segmentId = INVALID_SEGMENT;
                if (fixedSegmentId) {
                    return false;
                }
            } else if (segmentId != INVALID_SEGMENT) {
                if (!isLog) {
                    // allocate a group via direct mapping
                    groupId = segmentId;
                    assert(_metaMap.group.count(groupId) == 0);
                    _metaMap.group[groupId].first.segments.clear();
                    _metaMap.group[groupId].first.position.clear();
                    _metaMap.group[groupId].second = false;
                    resetGroupFronts(groupId, false);
                    resetGroupByteCounts(groupId, false);
                    _freeMainSegment--;
                } else {
                    _freeLogSegment--;
                }
                assert(!needsLock || _metaMap.group.at(groupId).second == false);
                assert(_metaMap.group.at(groupId).first.position.count(segmentId) == 0);
                _metaMap.group.at(groupId).first.position[segmentId] = _metaMap.group.at(groupId).first.segments.size();
                _metaMap.group.at(groupId).first.segments.push_back(segmentId);
                if (needsLock && _metaMap.group.at(groupId).second == false) {
                    _metaMap.group.at(groupId).second = needsLock;
                }
                _metaMap.segment[segmentId] = std::pair<group_id_t, bool> (groupId, false);
            }
        } while (retry < ConfigManager::getInstance().getRetryMax() && segmentId == INVALID_SEGMENT);
    }
    if (!fixedSegmentId) {
        assert(segmentId != INVALID_SEGMENT && groupId != INVALID_GROUP);
    }
    return segmentId != INVALID_SEGMENT && groupId != INVALID_GROUP;
}

bool SegmentGroupManager::freeGroup(group_id_t groupId, bool releaseLock) {
    std::lock_guard<std::mutex> lock(_metaMap.lock);
    if (groupId == INVALID_GROUP || _metaMap.group.count(groupId) <= 0 || (!releaseLock && _metaMap.segment.at(groupId).second)) {
        return false;
    }
    _bitmap.group->clearBit(groupId);
    _metaMap.group.erase(groupId);
    return true;
}

bool SegmentGroupManager::freeSegment(segment_id_t segmentId, bool releaseLock) {
    std::lock_guard<std::mutex> lock(_metaMap.lock);
    if (segmentId == INVALID_SEGMENT || _metaMap.segment.count(segmentId) <= 0 || (!releaseLock && _metaMap.segment.at(segmentId).second)) {
        // not exist or locked
        return false;
    }
    // Todo: group mapping?
    // mark the segment as free
    _bitmap.segment->clearBit(segmentId);
    _metaMap.segment.erase(segmentId);
    _metaMap.segmentFront.erase(segmentId);
    if (isLogSegment(segmentId)) {
        _freeLogSegment++;
    } else {
        _freeMainSegment++;
    }
    return true;
}

bool SegmentGroupManager::setSegmentFlushFront(segment_id_t segmentId, segment_len_t flushFront) {
    segment_len_t mainSegmentSize = ConfigManager::getInstance().getMainSegmentSize();
    segment_len_t logSegmentSize = ConfigManager::getInstance().getLogSegmentSize();
    if ((isLogSegment(segmentId) && flushFront > logSegmentSize) || (!isLogSegment(segmentId) && flushFront > mainSegmentSize)) {
        return false;
    }
    _metaMap.segmentFront[segmentId] = flushFront;
    return true;
}

segment_len_t SegmentGroupManager::getSegmentFlushFront(segment_id_t segmentId) {
    std::lock_guard<std::mutex> lk (_metaMap.lock);
    if (_metaMap.segmentFront.count(segmentId)) {
        return _metaMap.segmentFront.at(segmentId);
    } else {
        return INVALID_LEN;
    }
}

group_id_t SegmentGroupManager::getGroupBySegmentId(segment_id_t segmentId) {
    return (_metaMap.segment.count(segmentId) == 0? INVALID_GROUP : _metaMap.segment.at(segmentId).first);
    //return getMainSegmentBySegmentId(segmentId);
}

segment_id_t SegmentGroupManager::getMainSegmentBySegmentId(segment_id_t segmentId) {
    std::lock_guard<std::mutex> lk (_metaMap.lock);
    return (_metaMap.segment.count(segmentId) == 0 ? INVALID_SEGMENT : _metaMap.group.at(_metaMap.segment.at(segmentId).first).first.segments.at(0));
}

segment_id_t SegmentGroupManager::getGroupMainSegment(group_id_t groupId) {
    std::lock_guard<std::mutex> lk (_metaMap.lock);
    return (_metaMap.group.count(groupId) > 0 ? groupId : INVALID_SEGMENT);
}

bool SegmentGroupManager::accessGroupLock(group_id_t groupId, bool islock) {
    std::lock_guard<std::mutex> lk (_metaMap.lock);
    //debug_info ("group %lu tolock = %d curlock = %d\n", groupId, islock, _metaMap.group.at(groupId).second);
    if (_metaMap.group.count(groupId) == 0 || _metaMap.group.at(groupId).second == islock) {
        // not exist, or already in locked / unlocked status
        debug_warn("Group %s id=%lu, islock=%s\n", _metaMap.group.count(groupId) == 0? "not exist" : "action failed", groupId, islock? "true" : "false");
        //assert(0);
        return false;
    } else {
        // exist, not locked / unlocked -> lock / unlock it
        debug_info("Group %s id=%lu\n", islock? "lock" : "unlock", groupId);
        _metaMap.group.at(groupId).second = islock;
        return true;
    }
    return false;
}

bool SegmentGroupManager::getGroupLock(group_id_t groupId) {
    return accessGroupLock(groupId, true);
}

bool SegmentGroupManager::releaseGroupLock(group_id_t groupId) {
    return accessGroupLock(groupId, false);
}

offset_t SegmentGroupManager::getGroupFlushFront(group_id_t groupId, bool needsLock) {
    if (needsLock) getGroupLock(groupId);
    if (_metaMap.group.count(groupId) > 0) {
        return _metaMap.group.at(groupId).first.flushFront;
    }
    return INVALID_OFFSET;
}

offset_t SegmentGroupManager::getGroupWriteFront(group_id_t groupId, bool needsLock) {
    if (needsLock) getGroupLock(groupId);
    if (_metaMap.group.count(groupId) > 0) {
        return _metaMap.group.at(groupId).first.writeFront;
    }
    return INVALID_OFFSET;
}

offset_t SegmentGroupManager::setGroupFlushFront(group_id_t groupId, offset_t front, bool needsLock) {
    if (needsLock) getGroupLock(groupId);
    if (_metaMap.group.count(groupId) > 0) {
        _metaMap.group.at(groupId).first.flushFront = front;
        return front;
    }
    return INVALID_OFFSET;
}

offset_t SegmentGroupManager::resetGroupFronts(group_id_t groupId, bool needsLock) {
    return setGroupWriteFront(groupId, 0, needsLock) + setGroupFlushFront(groupId, 0, needsLock);
}

offset_t SegmentGroupManager::setGroupWriteFront(group_id_t groupId, offset_t front, bool needsLock) {
    if (needsLock) getGroupLock(groupId);
    if (_metaMap.group.count(groupId) > 0) {
        _metaMap.group.at(groupId).first.writeFront = front;
        if (front != 0) updateGroupWriteBackRatio(groupId);
        return front;
    }
    return INVALID_OFFSET;
}

std::vector<segment_id_t> SegmentGroupManager::getGroupSegments(group_id_t groupId, bool needsLock) {
    if (needsLock) getGroupLock(groupId);
    if (_metaMap.group.count(groupId) > 0) {
        return _metaMap.group.at(groupId).first.segments;
    }
    return std::vector<segment_id_t> ();
}

void SegmentGroupManager::resetGroupByteCounts(group_id_t groupId, bool needsLock) {
    if (needsLock) getGroupLock(groupId);
    if (_metaMap.group.count(groupId) > 0) {
        _metaMap.group.at(groupId).first.mainSegmentValidBytes = 0;
        _metaMap.group.at(groupId).first.maxMainSegmentBytes = 0;
    }
}

len_t SegmentGroupManager::addGroupMainSegmentBytes(group_id_t groupId, len_t diff, bool needsLock) {
    if (needsLock) getGroupLock(groupId);
    if (_metaMap.group.count(groupId) > 0) {
        _metaMap.group.at(groupId).first.mainSegmentValidBytes += diff;
        _metaMap.group.at(groupId).first.maxMainSegmentBytes += diff;
        updateGroupWriteBackRatio(groupId);
        return _metaMap.group.at(groupId).first.mainSegmentValidBytes;
    }
    return INVALID_LEN;
}

len_t SegmentGroupManager::removeGroupMainSegmentBytes(group_id_t groupId, len_t diff, bool needsLock) {
    if (needsLock) getGroupLock(groupId);
    if (_metaMap.group.count(groupId) > 0) {
        _metaMap.group.at(groupId).first.mainSegmentValidBytes -= diff;
        updateGroupWriteBackRatio(groupId);
        return _metaMap.group.at(groupId).first.mainSegmentValidBytes;
    }
    return INVALID_LEN;
}

len_t SegmentGroupManager::getGroupMainSegmentBytes(group_id_t groupId, bool needsLock) {
    if (needsLock) getGroupLock(groupId);
    if (_metaMap.group.count(groupId) > 0) {
        return _metaMap.group.at(groupId).first.mainSegmentValidBytes;
    }
    return INVALID_LEN;
}

len_t SegmentGroupManager::getGroupMainSegmentMaxBytes(group_id_t groupId, bool needsLock) {
    if (needsLock) getGroupLock(groupId);
    if (_metaMap.group.count(groupId) > 0) {
        return _metaMap.group.at(groupId).first.maxMainSegmentBytes;
    }
    return INVALID_LEN;
}

std::vector<segment_id_t> SegmentGroupManager::getGroupLogSegments(group_id_t groupId, bool needsLock) {
    std::vector<segment_id_t> logSegments;
    if (needsLock) getGroupLock(groupId);
    if (_metaMap.group.count(groupId) > 0) {
        logSegments.insert(logSegments.begin(), _metaMap.group.at(groupId).first.segments.begin()+1, _metaMap.group.at(groupId).first.segments.end());
    }
    return logSegments;
}

void SegmentGroupManager::releaseGroupLogSegments(group_id_t groupId, bool needsLock) {
    if (needsLock) getGroupLock(groupId);
    // release all segment, except main segment
    while (_metaMap.group.count(groupId) > 0 && _metaMap.group.at(groupId).first.segments.size() > 1) {
        segment_id_t lcid = _metaMap.group.at(groupId).first.segments.back();
        _metaMap.group.at(groupId).first.position.erase(lcid);
        freeSegment(lcid, /* releaseLock = */ true);
        _metaMap.group.at(groupId).first.segments.pop_back();
    }
    // reset the write and flush frontiers to cover the main segment only
    _metaMap.group.at(groupId).first.writeFront = ConfigManager::getInstance().getMainSegmentSize();
    _metaMap.group.at(groupId).first.flushFront = ConfigManager::getInstance().getMainSegmentSize();
    len_t maxMainSegmentBytes = _metaMap.group.at(groupId).first.maxMainSegmentBytes;
    resetGroupByteCounts(groupId, false);
    addGroupMainSegmentBytes(groupId, maxMainSegmentBytes);
}

bool SegmentGroupManager::setGroupSegments(group_id_t groupId, std::vector<segment_id_t> segments) {
    std::lock_guard<std::mutex> lk (_metaMap.lock);

    std::vector<segment_id_t> oldSegments = _metaMap.group[groupId].first.segments;
    for (auto c : oldSegments) {
        _bitmap.segment->clearBit(c);
        if (c < ConfigManager::getInstance().getNumMainSegment()) {
            _freeMainSegment++;
        } else {
            _freeLogSegment++;
        }
        _metaMap.segment.erase(c);
        _metaMap.group.at(groupId).first.position.erase(c);
    }
    size_t idx = 0;
    for (auto c : segments) {
        _bitmap.segment->setBit(c);
        if (c < ConfigManager::getInstance().getNumMainSegment()) {
            _freeMainSegment--;
        } else {
            _freeLogSegment--;
        }
        _metaMap.segment[c] = std::pair<group_id_t, bool>(groupId, false);
        _metaMap.group.at(groupId).first.position[c] = idx++;
    }
    _metaMap.group[groupId].first.segments = segments;
    return true;
}

len_t SegmentGroupManager::changeBytes(group_id_t groupId, len_t bytes, int type) {
    //while(!getGroupLock(groupId));

    len_t oldValue = _maxSpaceToRelease->getValue(groupId);
    // offset the old value in heap for stripes manually GC
    if (_groupInHeap.count(groupId) <= 0) {
        _groupInHeap.insert(groupId);
        bytes -= oldValue;
    }
    switch (type) {
        case 2:
            break;
        case 1:
            // record the amount of used space (freeable) from reserved area of segments / reserved segments
            STAT_TIME_PROCESS(_maxSpaceToRelease->update(groupId, 0, bytes), StatsType::GC_INVALID_BYTES_UPDATE);
            updateGroupWriteBackRatio(groupId);
            break;
        case 0:
            // record the amount of freeable space from data area of segments
            STAT_TIME_PROCESS(_maxSpaceToRelease->update(groupId, 0, bytes), StatsType::GC_INVALID_BYTES_UPDATE);
            updateGroupWriteBackRatio(groupId);
            break;
        default:
            assert(0);
            break;
    }

    //releaseGroupLock(groupId);

    return oldValue + bytes;
}

len_t SegmentGroupManager::releaseData(group_id_t groupId, len_t bytes) {
    StatsRecorder::getInstance()->totalProcess(StatsType::UPDATE_TO_MAIN, bytes);
    return changeBytes(groupId, bytes, 0);
}

len_t SegmentGroupManager::useReserved(group_id_t groupId, len_t bytes) {
    StatsRecorder::getInstance()->totalProcess(StatsType::UPDATE_TO_LOG, bytes);
    return changeBytes(groupId, bytes, 1);
}

double SegmentGroupManager::getGroupWriteBackRatio(group_id_t groupId, int type, bool isGC) {
    len_t mainSegmentSize = ConfigManager::getInstance().getMainSegmentSize();
    len_t flushFront = getGroupFlushFront(groupId, false);
    len_t validBytes = 0;
    len_t invalidBytes = 1;
    // (write back)/(reclaim)
    if (type == 0) {
        // all
        //validBytes = getGroupMainSegmentMaxBytes(groupId);
        //invalidBytes = (flushFront == INVALID_LEN || flushFront < validBytes)? 0 : flushFront - validBytes;
        validBytes = mainSegmentSize;
        invalidBytes = (flushFront > validBytes? flushFront - validBytes : 0);
    } else if (type == 1) {
        //validBytes = getGroupMainSegmentMaxBytes(groupId) - getGroupMainSegmentBytes(groupId);
        validBytes = flushFront - mainSegmentSize;
        // log only
        //if (flushFront != INVALID_LEN && flushFront > mainSegmentSize + validBytes) { // has log space, and some are invalid
        if (flushFront != INVALID_LEN && flushFront > 1.5 * mainSegmentSize) { // has log space, and some are invalid
            //invalidBytes = flushFront - mainSegmentSize - validBytes;
            invalidBytes = flushFront - 1.5 * mainSegmentSize;
        } else {
            validBytes = ULONG_MAX;
            invalidBytes = 0;
        }
    } else {
        // unknown scheme
        return DBL_MAX;
    }
    //if (isGC) printf("group %lu type %d valid = %lu invalid = %lu FF = %lu\n", groupId, type, validBytes, invalidBytes, flushFront);
    if (invalidBytes != 0 && validBytes > 0) {
        return validBytes * 1.0 / invalidBytes;
    } else {
        return DBL_MAX;
    }
}

void SegmentGroupManager::updateGroupWriteBackRatio(group_id_t groupId) {
    struct timeval startTime;
    gettimeofday(&startTime, 0);
    if (_groupInHeap.count(groupId) <= 0) {
        _groupInHeap.insert(groupId);
    }

    double all = getGroupWriteBackRatio(groupId, /* type = */ 0);
    double logOnly = getGroupWriteBackRatio(groupId, /* type = */ 1);
    // find the min ratio
    double ratio = (all <= 0.0 ? logOnly : (logOnly <= 0.0 ? all : std::min(all, logOnly)));
    // if all zero, then set it to max to avoid GC as far as possible
    if (ratio <= 0) {
        ratio = DBL_MAX;
    //} else {
    //    if (all != DBL_MAX && logOnly != DBL_MAX)
    //        printf("group %lu write back ratio a=%.3lf l=%.3lf r=%.3lf\n", groupId, all, logOnly, ratio);
    }
    _minWriteBackRatio->update(groupId, 0, ratio, /* newValue = */ true);
    StatsRecorder::getInstance()->timeProcess(StatsType::GC_RATIO_UPDATE, startTime);
}

bool SegmentGroupManager::convertRefMainSegment(ValueLocation &valueLoc, bool needsLock) {
    _metaMap.lock.lock();
    bool ret = false;
    group_id_t groupId = _metaMap.segment.at(valueLoc.segmentId).first;
    if (needsLock) {
        _metaMap.lock.unlock();
        ret = getGroupLock(groupId);
        if (!ret) return false;
        _metaMap.lock.lock();
    }

    assert(!_metaMap.group.at(groupId).first.segments.empty());
    segment_id_t mainSegmentId = _metaMap.group.at(groupId).first.segments.at(0);

    if (mainSegmentId != valueLoc.segmentId) {
        int pos = _metaMap.group.at(groupId).first.position.at(valueLoc.segmentId);
        valueLoc.segmentId = mainSegmentId;
        valueLoc.offset += ConfigManager::getInstance().getMainSegmentSize() + (pos - 1) * ConfigManager::getInstance().getLogSegmentSize();
    }
    _metaMap.lock.unlock();

    return true;
}

len_t SegmentGroupManager::getAndIncrementVLogWriteOffset(len_t diff, bool isGC) {
    return getAndIncrementVLogOffset(diff, 0 /* write offset */, isGC);
}

len_t SegmentGroupManager::getAndIncrementVLogGCOffset(len_t diff) {
    return getAndIncrementVLogOffset(diff, 1 /* gc offset */);
}

len_t SegmentGroupManager::getAndIncrementVLogOffset(len_t diff, int type, bool isGC) {
    len_t startingOffset = INVALID_LEN;
    len_t capacity = _isSlave ? ConfigManager::getInstance().getColdStorageCapacity() : ConfigManager::getInstance().getSystemEffectiveCapacity();
    len_t gcSize = ConfigManager::getInstance().getVLogGCSize();

    if (diff <= 0) {
        return startingOffset;
    }

    _metaMap.lock.lock();
    if (type == 0) { // write
        // make sure the data can fit
        assert(diff < capacity);

        len_t incOffset = (_vlog.writeFront + diff) % capacity;
        len_t gcFront = (_vlog.gcFront - (isGC? 0 : gcSize) + capacity) % capacity;
        if (
                (_vlog.writeFront >= gcFront && (incOffset > _vlog.writeFront || (_isSlave && incOffset <= gcFront) || (!_isSlave && incOffset < gcFront))) ||
                (_vlog.writeFront < gcFront && (incOffset > _vlog.writeFront && ((_isSlave && incOffset <= gcFront) || (!_isSlave && incOffset < gcFront))))
        ) {
            startingOffset = _vlog.writeFront;
            _vlog.writeFront = incOffset;
        }
        //printf("return %lu WF push to %lu\n", startingOffset, _vlog.writeFront);
    } else { // gc
        startingOffset = _vlog.gcFront;
        _vlog.gcFront = (_vlog.gcFront + diff) % capacity;
        //printf("GC return %lu and increment to %lu, wf %lu\n", startingOffset, _vlog.gcFront, _vlog.writeFront);
    }
    _metaMap.lock.unlock();
    return startingOffset;
}

len_t SegmentGroupManager::getLogWriteOffset() {
    return _vlog.writeFront;
}

len_t SegmentGroupManager::getLogGCOffset() {
    return _vlog.gcFront;
}

len_t SegmentGroupManager::getLogValidBytes() {
    std::string value = _keyManager->getMeta(LogValidByteString, strlen(LogValidByteString));
    return value.empty()? 0 : stoul(value);
}

len_t SegmentGroupManager::getLogWrittenBytes() {
    std::string value = _keyManager->getMeta(LogWrittenByteString, strlen(LogWrittenByteString));
    return value.empty()? 0 : stoul(value);
}

void SegmentGroupManager::printUsage(FILE *out) {
    segment_id_t numMainSegment = ConfigManager::getInstance().getNumMainSegment();
    segment_id_t numLogSegment = ConfigManager::getInstance().getNumLogSegment();
    if (ConfigManager::getInstance().enabledVLogMode()) {
        fprintf(out, 
                "Log Usage:\n"
                " Head: %lu; Tail %lu\n"
                , _vlog.gcFront
                , _vlog.writeFront
               );
    } else {
        fprintf(out, 
                "Storage Usage:\n"
                "  Total: %lu; In use: %lu (%3.2f%%)\n"
                "  Main: %lu; In use: %lu (%3.2f%%)\n"
                "  Log: %lu; In use: %lu (%3.2f%%)\n"
                , _MaxSegment
                , _metaMap.segment.size()
                , double(_metaMap.segment.size()) / _MaxSegment * 100
                , numMainSegment
                , numMainSegment - _freeMainSegment
                , (1 - double(_freeMainSegment) / numMainSegment) * 100
                , numLogSegment
                , numLogSegment - _freeLogSegment
                , (1 - double(_freeLogSegment) / numLogSegment) * 100
                );
    }
}

void SegmentGroupManager::printGroups(FILE *out) {
    for (auto g : _metaMap.group) {
        fprintf(out,
                "Group=%lu, write=%lu flush=%lu segments=%lu validBytes=%lu(%lu) locked=%s\n"
                , g.first
                , g.second.first.writeFront
                , g.second.first.flushFront
                , g.second.first.segments.size()
                , g.second.first.maxMainSegmentBytes
                , g.second.first.mainSegmentValidBytes
                , g.second.second? "true" : "false"
        );
    }
}

double SegmentGroupManager::getCurrentUsage() {
    return double(_metaMap.segment.size()) / _MaxSegment * 100;
}

segment_id_t SegmentGroupManager::getNumFreeLogSegments() {
    return getNumFreeSegments(2);
}

segment_id_t SegmentGroupManager::getNumFreeMainSegments() {
    return getNumFreeSegments(1);
}

segment_id_t SegmentGroupManager::getNumFreeSegments() {
    return getNumFreeSegments(0);
}

segment_id_t SegmentGroupManager::getNumFreeSegments(int type) {
    switch (type) {
        case 1: // MAIN
            return _freeMainSegment;
        case 2: // LOG
            if (_freeLogSegment > ConfigManager::getInstance().getNumLogSegment()) {
                debug_error("Invalid number of free log segments %lu\n", _freeLogSegment);
            }
            return _freeLogSegment;
        case 0: // ALL
        default:
            return _MaxSegment - _metaMap.segment.size();
    }
    return 0;
}

bool SegmentGroupManager::isLogSegment(segment_id_t segmentId) {
    segment_len_t numMainSegment = ConfigManager::getInstance().getNumMainSegment();
    return segmentId >= numMainSegment;
}

bool SegmentGroupManager::restoreMetaFromDB() {
    std::lock_guard<std::mutex> lk (_metaMap.lock);

    bool restored = false;
    if (_keyManager == 0)
        return restored;

    std::set<segment_id_t> segments;

    // group metadata
    for (group_id_t gid = 0; gid < ConfigManager::getInstance().getNumMainSegment(); gid++) {
        std::string key = getGroupKey(gid);
        std::string metadata = _keyManager->getMeta(key.c_str(), key.length());
        //printf("group %lu meta %s\n", gid, metadata.c_str());
        if (!metadata.empty()) {
            // find the start of the segment list
            size_t spos = 0, epos = 0, cpos = 0;
            epos = metadata.find_first_of(ListSeparator, 0);
            offset_t writeFront = std::stoul(metadata.substr(/* start = */ spos, /* length = */ epos - spos));
            spos = epos;
            // list of segments
            GroupMetaData groupMeta;
            groupMeta.mainSegmentValidBytes = 0;
            groupMeta.maxMainSegmentBytes = 0;
            while (spos < metadata.length()) {
                NEXT_SUB_STR();
                //printf("%s s: %lu e : %lu\n", metadata.substr(spos + 1, epos - (spos + 1)).c_str(), spos, epos);
                segment_id_t segmentId = std::stoul(metadata.substr(spos + 1, epos - (spos + 1)));
                groupMeta.segments.push_back(segmentId);
                groupMeta.position[segmentId] = cpos;
                _bitmap.segment->setBit(segmentId);
                _metaMap.segment[segmentId] = std::pair<group_id_t, bool> (gid, false);
                if (segmentId < ConfigManager::getInstance().getNumMainSegment()) {
                    _freeMainSegment--;
                } else {
                    _freeLogSegment--;
                }
                segments.insert(segmentId);
                // end becomes the front
                spos = epos;
                cpos++;
            }
            groupMeta.writeFront = writeFront;
            groupMeta.flushFront = writeFront;
            _metaMap.group[gid] = std::pair<GroupMetaData, bool>(groupMeta, false);
            // heap for GC
            useReserved(gid, writeFront);
        }
        _bitmap.group->setBit(gid);
    }

    // segment metadata
    for (auto cid : segments) {
        std::string key = getSegmentKey(cid);
        std::string metadata = _keyManager->getMeta(key.c_str(), key.length());
        //printf("c %lu meta %s\n", cid, metadata.c_str());
        if (!metadata.empty()) {
            _metaMap.segmentFront[cid] = std::stoul(metadata);
        }
    }


    return restored;
}

std::string SegmentGroupManager::getGroupKey(group_id_t groupId) {
    std::string key;
    key.append(GroupPrefix);
    key.append(to_string(groupId));

    return key;

}

std::string SegmentGroupManager::getSegmentKey(segment_id_t segmentId) {
    std::string key;
    key.append(SegmentPrefix);
    key.append(to_string(segmentId));

    return key;
}

std::string SegmentGroupManager::generateSegmentValue(offset_t writeFront) {
    return std::string(to_string(writeFront));
}

std::string SegmentGroupManager::generateGroupValue(offset_t writeFront, std::vector<segment_id_t> segments) {
    std::string value (to_string(writeFront));
    for (segment_id_t &cid : segments) {
        value.append(ListSeparator);
        value.append(to_string(cid));
    }
    return value;
}

bool SegmentGroupManager::writeGroupMeta(group_id_t groupId) {
    std::string key = getGroupKey(groupId);
    return _keyManager->writeMeta(
            key.c_str(),
            key.length(),
            generateGroupValue(
                    getGroupWriteFront(groupId, /* needsLock = */ false),
                    getGroupSegments(groupId, /* needsLock = */ false)
            )
    );
}

bool SegmentGroupManager::writeSegmentMeta(segment_id_t segmentId) {
    std::string key = getSegmentKey(segmentId);
    return _keyManager->writeMeta(
            key.c_str(),
            key.length(),
            generateSegmentValue(getSegmentFlushFront(segmentId))
    );
}

bool SegmentGroupManager::readGroupMeta(const std::string &metadata, offset_t &writeFront, std::vector<segment_id_t> &segments) {
    size_t spos = 0, epos = 0;

    epos = metadata.find_first_of(ListSeparator, 0);
    if (epos == std::string::npos)
        epos = metadata.length() + 1;
    writeFront = std::stoul(metadata.substr(/* start = */ spos, /* length = */ epos - spos));

    spos = epos;
    while (spos < metadata.length()) {
        NEXT_SUB_STR();
        segments.push_back(std::stoul(metadata.substr(spos + 1, epos - (spos + 1))));
        // end becomes the front
        spos = epos;
    }

    return true;
}

#undef NEXT_SUB_STR

