#include <vector>
#include <unordered_set>
#include <float.h>
#include "gcManager.hh"
#include "configManager.hh"
#include "statsRecorder.hh"

#define TAG_MASK  (1000 * 1000)

GCManager::GCManager(KeyManager *keyManager, ValueManager *valueManager, DeviceManager *deviceManager, SegmentGroupManager *segmentGroupManager, bool isSlave):
        _keyManager(keyManager), _valueManager(valueManager), _deviceManager(deviceManager), _segmentGroupManager(segmentGroupManager) {
    _maxGC = ConfigManager::getInstance().getGreedyGCSize();
    _useMmap = ConfigManager::getInstance().useMmap();
    if ((ConfigManager::getInstance().enabledVLogMode() && !_useMmap)|| isSlave) {
        Segment::init(_gcSegment.read, INVALID_SEGMENT, ConfigManager::getInstance().getVLogGCSize());
    }
    Segment::init(_gcSegment.write, INVALID_SEGMENT, ConfigManager::getInstance().getVLogGCSize());
    _isSlave = isSlave;
    _gcCount.ops = 0;
    _gcCount.groups = 0;
    _gcCount.scanSize = 0;
    _gcWriteBackBytes = 0;

    _gcReadthreads.size_controller().resize(ConfigManager::getInstance().getNumGCReadThread());
}

GCManager::~GCManager() {
    // for debug and measurement 
    printStats();
}

void GCManager::printStats(FILE *out) {
    fprintf(out, "_gcBytes writeBack = %lu scan = %lu\n", _gcWriteBackBytes, _gcCount.scanSize);
    fprintf(out, "Mode counts (total ops = %lu groups = %lu):\n", _gcCount.ops, _gcCount.groups);
    for (auto it : _modeCount) {
        fprintf(out, "[%d] = %lu\n", it.first, it.second);
    }
}

size_t GCManager::gcGreedy(bool needsGCLock, bool needsLockCentralizedReservedPool, group_id_t *reportGroupId) {
    size_t gcBytes = 0;
    std::vector<std::pair<group_id_t, len_t> > gcGroups;
    std::pair<group_id_t, len_t> gcGroup;
    group_id_t gcGroupId;
    len_t bytes = 0;
    double ratio = DBL_MAX;
    std::unordered_set<group_id_t> gcGroupIds;
    if (needsGCLock) _valueManager->_GCLock.lock();
    GCMode defaultGCMode = ConfigManager::getInstance().getGCMode();
    //_segmentGroupManager->_maxSpaceToRelease->print(stdout);
    //_segmentGroupManager->_minWriteBackSize->print();
    // get the group with most free data space, and reserved space/segments to release
    for (int i = 0; i < _maxGC; i++) {
        switch(defaultGCMode) {
            case LOG_ONLY:
                tie(gcGroupId, ratio) = _segmentGroupManager->_minWriteBackRatio->getMin();
                //printf("Selected group %lu ratio %lf\n", gcGroupId, ratio);
                break;
            case ALL:
            default:
                tie(gcGroupId, bytes) = _segmentGroupManager->_maxSpaceToRelease->getMax();
                //printf("Select group %lu with bytes %lu\n", gcGroupId, bytes);
                break;
        }
        if (gcGroupId == INVALID_GROUP) {
            break;
        }
        // skip if the group does not exist, or is yet sealed, or nothing to reclaim from group
        if (_segmentGroupManager->_groupInHeap.count(gcGroupId) <= 0 ||
                (defaultGCMode == LOG_ONLY && ratio == DBL_MAX)) {
            i--;
            continue;
        }
        //printf("GC group %lu with value %lu (ratio = %lf)\n", gcGroupId, bytes, ratio);
        assert(gcGroupIds.count(gcGroupId) == 0);
        if (gcGroupIds.count(gcGroupId) == 0) {
            gcGroup.first = gcGroupId;
            gcGroup.second = bytes;
            gcGroups.push_back(gcGroup);
        }
        gcGroupIds.insert(gcGroupId);
    }
    assert(!gcGroups.empty());
    GCMode gcMode = ALL;
    struct timeval gcStartTime;
    gettimeofday(&gcStartTime, 0);
    // perform GC on the groups one by one
    for (unsigned long i = 0; i < gcGroups.size(); i++) {
        gcGroup = gcGroups[i];
        //printf("GC group %lu\n", gcGroup.first);
        gcBytes += gcOneGroup(gcGroup.first, gcMode, needsLockCentralizedReservedPool, gcGroup.second, reportGroupId);
        _gcCount.groups++;
        if (reportGroupId != 0 && *reportGroupId == gcGroup.first) {
            if (isLogOnly(gcMode)) {
                _segmentGroupManager->releaseGroupLock(gcGroup.first);
            }
            *reportGroupId = INVALID_GROUP;
        }
    }
    StatsRecorder::getInstance()->timeProcess(StatsType::GC_TOTAL, gcStartTime);
    //printf("END of GC group %lu\n", gcGroups.size());
    // Todo Remove the groups from both heaps
    if (needsGCLock) _valueManager->_GCLock.unlock();
    _gcCount.ops++;
    return gcBytes;
}

size_t GCManager::gcAll() {
    int oldMaxGC = _maxGC;
    _maxGC = ConfigManager::getInstance().getNumMainSegment();
    size_t gcBytes = gcGreedy();
    std::swap(_maxGC, oldMaxGC);
    return gcBytes;
}

size_t GCManager::gcVLog() {

    _gcCount.ops++;
    //_valueManager->_GCLock.lock();

    size_t gcBytes = 0, gcScanSize = 0;
    len_t gcSize = ConfigManager::getInstance().getVLogGCSize();
    unsigned char *readPool = _useMmap? 0 : Segment::getData(_gcSegment.read);
    unsigned char *writePool = Segment::getData(_gcSegment.write);

    std::vector<char*> keys;
    std::vector<ValueLocation> values;
    ValueLocation valueLoc;
    valueLoc.segmentId = 0;

    len_t capacity = ConfigManager::getInstance().getSystemEffectiveCapacity();
    len_t len = INVALID_LEN;
    offset_t logOffset = INVALID_OFFSET, gcFront = INVALID_OFFSET, flushFront = 0;
    size_t remains = 0;
    bool ret = false;
    const segment_id_t maxSegment = ConfigManager::getInstance().getNumSegment();

    struct timeval gcStartTime;
    gettimeofday(&gcStartTime, 0);
    // scan until the designated reclaim size is reached
    while (gcBytes < gcSize) {
        // see if there is anything left over in last scan
        flushFront = Segment::getFlushFront(_gcSegment.read);
        assert(flushFront != gcSize);
        // read and fit up only the available part of buffer
        gcFront = _segmentGroupManager->getAndIncrementVLogGCOffset(gcSize - flushFront);
        if (_useMmap) {
            readPool = _deviceManager->readMmap(0, gcFront - flushFront, gcSize, 0);
            Segment::init(_gcSegment.read, 0, readPool, gcSize);
        } else {
            _deviceManager->readAhead(/* diskId = */ 0, gcFront, gcSize - flushFront);
            STAT_TIME_PROCESS(_deviceManager->readDisk(/* diskId = */ 0, readPool + flushFront, gcFront, gcSize - flushFront), StatsType::GC_READ);
        }
        // mark the amount of data in buffer
        Segment::setWriteFront(_gcSegment.read, gcSize);
        for (remains = gcSize; remains > 0;) {
            len_t valueSize = INVALID_LEN;
            offset_t keyOffset = gcSize - remains;
            off_len_t offLen (keyOffset + KEY_SIZE, sizeof(len_t));
            // check if we can read the value length
            if (KEY_SIZE + sizeof(len_t) > remains) {
                break;
            }
            Segment::readData(_gcSegment.read, &valueSize, offLen);
            // check if we can read the value
            if (KEY_SIZE + sizeof(len_t) + valueSize > remains) {
                break;
            }
            STAT_TIME_PROCESS(valueLoc = _keyManager->getKey((char*) readPool + keyOffset), StatsType::GC_KEY_LOOKUP);
            // check if pair is valid, avoid underflow by adding capacity, and overflow by modulation
            if (valueLoc.segmentId == (_isSlave? maxSegment : 0) && valueLoc.offset == (gcFront - flushFront + keyOffset + capacity) % capacity) {
                // buffer full, flush before write
                if (!Segment::canFit(_gcSegment.write, KEY_SIZE + sizeof(len_t) + valueSize)) {
                    STAT_TIME_PROCESS(tie(logOffset, len) = _valueManager->flushSegmentToWriteFront(_gcSegment.write, /* isGC = */ true), StatsType::GC_FLUSH);
                    assert(len > 0);
                    // update metadata
                    for (auto &v : values) {
                        v.offset = (v.offset + logOffset) % capacity;
                    }
                    STAT_TIME_PROCESS(ret = _keyManager->writeKeyBatch(keys, values), StatsType::UPDATE_KEY_WRITE_LSM_GC);
                    if (!ret) {
                        debug_error("Failed to update %lu keys to LSM\n", keys.size());
                        assert(0);
                        exit(-1);
                    }
                    // reset keys and value holders, buffers
                    keys.clear();
                    values.clear();
                    Segment::resetFronts(_gcSegment.write);
                }
                // mark and copy the key-value pair to tbe buffer
                offset_t writeKeyOffset = Segment::getWriteFront(_gcSegment.write);
                Segment::appendData(_gcSegment.write, readPool + keyOffset, KEY_SIZE + sizeof(len_t) + valueSize);
                keys.push_back((char*) writePool + writeKeyOffset);
                valueLoc.offset = writeKeyOffset;
                valueLoc.length = valueSize;
                values.push_back(valueLoc);
                _gcWriteBackBytes += KEY_SIZE + sizeof(len_t) + valueSize;
            } else {
                // space is reclaimed directly
                gcBytes += KEY_SIZE + sizeof(len_t) + valueSize;
                // cold storage invalid bytes cleared
                if (_isSlave) {
                    _valueManager->_slave.writtenBytes -= KEY_SIZE + sizeof(len_t) + valueSize;
                }
            }
            
            remains -= KEY_SIZE + sizeof(len_t) + valueSize;
        }
        if (remains > 0) {
            Segment::setFlushFront(_gcSegment.read, remains);
            if (!_useMmap) {
                // if some data left without scanning, move them to the front of buffer for next scan 
                memmove(readPool, readPool + gcSize - remains, remains);
            }
        } else {
            // reset buffer 
            Segment::resetFronts(_gcSegment.read);
        }
        if (_useMmap) {
            _deviceManager->readUmmap(0, gcFront, gcSize, readPool);
        }

        gcScanSize += gcSize;
    }
    // final check on remaining data to flush
    if (!keys.empty()) {
        STAT_TIME_PROCESS(tie(logOffset, len) = _valueManager->flushSegmentToWriteFront(_gcSegment.write, /* isGC = */ true), StatsType::GC_FLUSH);
        for (auto &v : values) {
            v.offset = (v.offset + logOffset) % capacity;
        }
        // update metadata of flushed data
        STAT_TIME_PROCESS(ret = _keyManager->writeKeyBatch(keys, values), StatsType::UPDATE_KEY_WRITE_LSM_GC);
        if (!ret) {
            debug_error("Failed to update %lu keys to LSM\n", keys.size());
            assert(0);
            exit(-1);
        }
        if (ConfigManager::getInstance().persistLogMeta()) {
            std::string value;
            value.append(to_string(logOffset + len));
            _keyManager->writeMeta(SegmentGroupManager::LogTailString, strlen(SegmentGroupManager::LogTailString), value);
        }
    }
    //printf("gcBytes %lu\n", gcBytes);
    StatsRecorder::getInstance()->totalProcess(StatsType::GC_SCAN_BYTES, gcScanSize);
    StatsRecorder::getInstance()->totalProcess(StatsType::GC_WRITE_BYTES, gcScanSize - gcBytes);
    // reset write buffer
    Segment::resetFronts(_gcSegment.write);

    // check if data read is not scanned or GCed, adjust the gc frontier accordingly
    if (remains > 0) {
        assert(0);
        exit(1);
        gcFront = _segmentGroupManager->getAndIncrementVLogGCOffset(gcSize - remains);
    }
    if (ConfigManager::getInstance().persistLogMeta()) {
        std::string value;
        value.append(to_string(gcFront + gcSize - remains));
        _keyManager->writeMeta(SegmentGroupManager::LogHeadString, strlen(SegmentGroupManager::LogHeadString), value);
    }
    // reset read buffer
    Segment::resetFronts(_gcSegment.read);
    StatsRecorder::getInstance()->timeProcess(StatsType::GC_TOTAL, gcStartTime);

    _gcCount.scanSize += gcScanSize;
    //_valueManager->_GCLock.unlock();
    return gcBytes;
}

/*
size_t GCManager::gcGroup(group_id_t groupId, bool needsLockCentralizedReservedPool) {
    _valueManager->_GCLock.lock();
    size_t gcBytes = gcOneGroup(groupId, needsLockCentralizedReservedPool);
    _valueManager->_GCLock.unlock();
    return gcBytes;
}
*/

size_t GCManager::gcOneGroup(group_id_t groupId, GCMode &finalGCMode, bool needsLockCentralizedReservedPool, len_t originBytes, group_id_t* reportGroupId) {

    ConfigManager &cm = ConfigManager::getInstance();
    _segmentGroupManager->_groupInHeap.erase(groupId);
    int freeLogSegments = 0;

    std::vector<segment_id_t> logSegments = _segmentGroupManager->getGroupSegments(groupId, reportGroupId == 0 || *reportGroupId != groupId);
    logSegments.erase(logSegments.begin());
    segment_id_t mainSegmentId = _segmentGroupManager->getGroupMainSegment(groupId);

    len_t mainSegmentSize = cm.getMainSegmentSize();
    len_t logSegmentSize = cm.getLogSegmentSize();

    GCMode gcMode = getGCMode(groupId, originBytes);
    if (!isLogOnly(gcMode)) {
        // pull the updates in the centralized buffer to a separate segment buffer for scanning
        bool done = _valueManager->prepareGCGroupInCentralizedPool(groupId);
        assert(done);
        if (done == false) {
            debug_error("Some updates cannot be GCed in buffer for group %lu\n", groupId);
        }
    }

    bool useMmap = cm.useMmap();
    //printf("GC group %lu with mode %d reportGroupId = %lu (ob=%lu)\n", groupId, gcMode, (reportGroupId? *reportGroupId : 0), originBytes);
    size_t gcBytes = 0;
    size_t bytesScanned = 0, bytesWritten = 0, validBytes = 0;
    bool dataInBuffer = false;
    // scan the all data segments
    std::vector<Segment> all;
    // storing the keys to write after GC
    unordered_map<unsigned char *, std::pair<int, ValueLocation>, hashKey, equalKey> keyCount;
    // main and reserved groups
    segment_id_t cid = mainSegmentId;
    unsigned int mx = 0, mn = UINT_MAX;

    std::vector<uint8_t> read;
    read.resize(1 + logSegments.size());
    struct timeval readStartTime;
    gettimeofday(&readStartTime, 0);

    for (size_t gcount = 0, cur = 0; gcount < 1 + logSegments.size(); gcount++) {

        Segment segment;
        // determine the next segment to read 
        if (gcount > 0) {
            cid = logSegments.at(gcount - 1);
        }

        // skip main segment for log only scan
        if (gcount == 0 && isLogOnly(gcMode)) {
            read.pop_back();
            continue;
        }

        // temp segment to hold kv-pairs
        len_t csize = (gcount == 0)? mainSegmentSize : logSegmentSize;
        if (useMmap) {
            unsigned char *data = _deviceManager->readMmap(cid, 0, csize, 0);
            Segment::init(segment, cid, data, csize);
        } else {
            Segment::init(segment, cid, csize, false);
        }
        // mark the amount of data exists in segment
        Segment::setFlushFront(segment, _segmentGroupManager->getSegmentFlushFront(cid));
        //printf("Scan group %lu [L=%d] segment %lu FlushFront %lu\n", groupId, gcount > 0, cid, _segmentGroupManager->getSegmentFlushFront(cid));
            
        // read from disk, and buffer whichever appropriate (1) data + reserved from disk, or (2) data from disk, reserved from buffer
        //unsigned char *segmentData = Segment::getData(segment);
        // copy if segment data in buf, else read from disk
        // a group may be flushed while GC is in process (?)
        // read segments from disk

        all.push_back(segment);

        if (!useMmap) {
            _deviceManager->readAhead(cid, 0, Segment::getFlushFront(segment));
            _gcReadthreads.schedule(
                    std::bind(
                        &DeviceManager::readPartialSegmentMtD,
                        _deviceManager,
                        cid,
                        0,
                        Segment::getFlushFront(segment),
                        Segment::getData(segment),
                        boost::ref(read.at(cur))
                    )
            );
            cur++;
        }
        //_valueManager->_deviceManager->readPartialSegment(cid, 0, Segment::getFlushFront(segment), segmentData);
        //Segment::dumpSegment(segment, true);

    }

    StatsRecorder::getInstance()->timeProcess(StatsType::GC_READ, readStartTime);

    for (size_t gcount = 0, cur = 0; gcount < 1 + logSegments.size(); gcount++) {

        // skip main segment for log only scan
        if (gcount == 0 && isLogOnly(gcMode)) {
            continue;
        }

        Segment segment = all.at(cur);
        // wait until the segment is read from disk
        gettimeofday(&readStartTime, 0);
        while(read.at(cur) == false && useMmap == false);
        StatsRecorder::getInstance()->timeProcess(StatsType::GC_READ, readStartTime);
        cur++;
            
        size_t scanLength = dataInBuffer? Segment::getWriteFront(segment) : Segment::getFlushFront(segment);

        bytesScanned += gcSegment(groupId, segment, keyCount, scanLength, /* isRemove = */ false, /* reservedPos = */ gcount, gcMode, &validBytes);

        if (gcount > 0) {
            freeLogSegments++;
        }
    }

    bool lastNoMap = false;
    // scan the unified buffer for updates after processing all reserved segments
    if (_valueManager->_segmentReservedByGroup.count(groupId) > 0) {
        // prepare the segments
        Segment cs;
        len_t lastFlushFront = _valueManager->getLastSegmentFront(_segmentGroupManager->getGroupFlushFront(groupId, false)) % logSegmentSize; 
        assert(
                (lastFlushFront <= mainSegmentSize && logSegments.empty()) ||
                (lastFlushFront <= logSegmentSize && !logSegments.empty())
              );
        if (logSegments.empty()) {
            // no log segment before hand
            lastFlushFront = 0;
        }
        Segment::dup(cs, _valueManager->_segmentReservedInBuf.at(mainSegmentId).segment);
        // clean leading bytes
        Segment::clean(cs, 0, lastFlushFront, false);
        //printf("gc in memory updates for segment %lu lastFlushFront %lu flushFront %lu writeFront %lu\n", mainSegmentId, lastFlushFront, Segment::getFlushFront(cs), Segment::getWriteFront(cs));
        // avoid any trailing bytes
        bytesScanned += gcSegment(groupId, cs, keyCount, Segment::getWriteFront(cs), false, logSegments.size() + 1, gcMode, &validBytes);
        all.push_back(cs);
        lastNoMap = true;
    }

    // release the log segments in metadata when log is rewritten
    if (isLogOnly(gcMode) && logSegments.size() * logSegmentSize < validBytes) {
        debug_error("Log space overflows for group %lu\n", groupId);
        assert(0);
        exit(-1);
    }

    // release the log segments
    _segmentGroupManager->releaseGroupLogSegments(groupId, /* needsLock = */ false);
    // reset the group fronts if the whole group is GCed
    if (!isLogOnly(gcMode)) {
        _segmentGroupManager->resetGroupFronts(groupId, /* needsLock = */ false);
    }

    //printf("Group %lu gc log = %d\n", groupId, isLogOnly(gcMode));

    // put and align all scanned data to a designated buffer (that can always hold all data in a group)
    len_t valueSize = 0;
    const len_t zero = 0;
    len_t recordSize = 0;
    unsigned int coldCount = 0;

    struct timeval gcFlushStartTime;
    gettimeofday(&gcFlushStartTime, 0);

    int numPipelinedBuffer = cm.getNumPipelinedBuffer();
    std::unordered_map<unsigned char*, offset_t, hashKey, equalKey> oldLocations;
    // put and aligned GCed data into flush buffer
    for (auto &it : keyCount) {
        // get the value size
        memcpy(&valueSize, it.first + KEY_SIZE, sizeof(len_t));
        bool writeToHotStorage = 
                !cm.useSlave() /* no cold storage */ ||
                getHotness(groupId, it.second.first % TAG_MASK) /* is hot key */ ||
                (cm.getColdStorageCapacity() < _valueManager->_slaveValueManager->_slave.writtenBytes + cm.getVLogGCSize() + KEY_SIZE + sizeof(len_t) + valueSize /* cold storage is full */ && 
                    valueSize > 0 /* not a tag */);
        // append updates back to a unique buffer
        Segment &pool = _valueManager->_centralizedReservedPool[numPipelinedBuffer].pool;
        // if the buffer is full before flush, flush before putting updates
        if (!Segment::canFit(pool, KEY_SIZE + sizeof(len_t) + valueSize)) {
            _valueManager->flushCentralizedReservedPool(reportGroupId, /* isUpdate */ false, /* poolIndex = */ numPipelinedBuffer);
        }
        std::pair<unsigned char*, segment_len_t> updatePair (Segment::getData(pool) + Segment::getWriteFront(pool), Segment::getWriteFront(pool));
        // put the update into buffer
        Segment::appendData(pool, it.first, KEY_SIZE);
        if (writeToHotStorage) {
            Segment::appendData(pool, it.first + KEY_SIZE, sizeof(len_t));
            if (valueSize > 0) { // not a tag
                Segment::appendData(pool, it.first + KEY_SIZE + sizeof(len_t), valueSize);
            } else {
                assert(0);
            }
            // originally tagged in cold storage, now move back to hot
            if (it.second.first >= TAG_MASK) {
                _valueManager->_slave.validBytes -= KEY_SIZE + sizeof(len_t) + valueSize;
            }
            recordSize = KEY_SIZE + sizeof(len_t) + valueSize;
        } else { // tag the key existance
            Segment::appendData(pool, &zero, sizeof(len_t));
            ValueLocation oldValueLoc;
            oldValueLoc.segmentId = 0;
            if (valueSize > 0) {
                _valueManager->_slaveValueManager->putValue((char*) it.first, KEY_SIZE, (char*) it.first + KEY_SIZE + sizeof(len_t), valueSize, oldValueLoc, 1);
                _valueManager->_slaveValueManager->_slave.writtenBytes += KEY_SIZE + sizeof(len_t) + valueSize;
                _valueManager->_slaveValueManager->_slave.validBytes += KEY_SIZE + sizeof(len_t) + valueSize;
                recordSize = (KEY_SIZE + sizeof(len_t)) * 2 + valueSize;
                coldCount++;
            } else {
                recordSize = KEY_SIZE + sizeof(len_t);
            }
            // update counter of GC write back
        }
        // setup the mapping of updates in buffer
        _valueManager->_centralizedReservedPool[numPipelinedBuffer].keysInPool.insert(updatePair);
        _valueManager->_centralizedReservedPool[numPipelinedBuffer].groupsInPool[groupId].insert(KEY_SIZE + sizeof(len_t) + valueSize);
        _valueManager->_centralizedReservedPool[numPipelinedBuffer].segmentsInPool[mainSegmentId].first += KEY_SIZE + sizeof(len_t) + valueSize;
        _valueManager->_centralizedReservedPool[numPipelinedBuffer].segmentsInPool[mainSegmentId].second.insert(updatePair.second);
        // update counter of GC write back
        _gcWriteBackBytes += recordSize;
        bytesWritten += recordSize;
        //_segmentGroupManager->removeGroupMainSegmentBytes(groupId, recordSize);
        oldLocations.insert(std::pair<unsigned char*, offset_t>(it.first, it.second.second.offset));
    }

    StatsRecorder::getInstance()->timeProcess(StatsType::GC_PRE_FLUSH, gcFlushStartTime);

    // flush all GCed data to disk
    if (Segment::getWriteFront(_valueManager->_centralizedReservedPool[numPipelinedBuffer].pool) > 0) {
        // clean up before flush to avoid segment retention
        if (!isLogOnly(gcMode)) {
            _valueManager->cleanupGCGroupInCentralizedPool(groupId, true, needsLockCentralizedReservedPool);
        }
        _valueManager->flushCentralizedReservedPool(reportGroupId, /* isUpdate */ false, /* poolIndex = */ numPipelinedBuffer, &oldLocations);
        if (coldCount > 0) {
            _valueManager->_slaveValueManager->forceSync();
        }
    }

    StatsRecorder::getInstance()->timeProcess(StatsType::GC_FLUSH, gcFlushStartTime);

    // free all buffered segments
    size_t ccount = 0;
    for (auto c : all) {
        ccount ++;
        if (useMmap && (ccount != all.size() || !lastNoMap)) {
            _deviceManager->readUmmap(Segment::getId(c), 0, Segment::getFlushFront(c), Segment::getData(c));
        } else {
            Segment::free(c);
        }
    }

    keyCount.clear();

    StatsRecorder::getInstance()->minMaxGCUpdate(mn, mx);

    gcBytes = bytesScanned - bytesWritten;
    //assert(gcBytes > 0);

    // clean up the updates in centralized pool after GC
    if (isLogOnly(gcMode)) {
        _valueManager->_segmentReservedPool->releaseSegment(_segmentGroupManager->getGroupMainSegment(groupId));
    }

    // mark the group as free
    if (reportGroupId == 0 || *reportGroupId != groupId) {
        _segmentGroupManager->releaseGroupLock(groupId);
        //printf("Release GC group %lu lock\n", groupId);
    }

    //printf("GC group %lu gc %lu bytesWritten %lu gcMode %d freeLogSegments %d origin %lu\n", groupId, gcBytes, bytesWritten, gcMode, freeLogSegments, originBytes);
    // record bytes scanned and written back
    StatsRecorder::getInstance()->totalProcess(StatsType::GC_SCAN_BYTES, bytesScanned);
    StatsRecorder::getInstance()->totalProcess(StatsType::GC_WRITE_BYTES, bytesWritten);
    //printf("GC group %lu mode %d Scanned %lu Written %lu\n", groupId, gcMode, bytesScanned, bytesWritten);
    finalGCMode = gcMode;
    if (_modeCount.count(gcMode) == 0) {
        _modeCount[gcMode] = 0;
    }
    _modeCount.at(gcMode)++;
    _gcCount.scanSize += bytesScanned;
    return gcBytes;
}

size_t GCManager::gcSegment(group_id_t groupId, Segment &segment, std::unordered_map<unsigned char *, std::pair<int, ValueLocation>, hashKey, equalKey> &keyCount, len_t total, bool isRemove, size_t reservedPos, int gcMode, size_t *validBytes) {
    len_t mainSegmentSize = ConfigManager::getInstance().getMainSegmentSize();
    len_t logSegmentSize = ConfigManager::getInstance().getLogSegmentSize();

    size_t bytesScanned = 0;

    // scan the whole segment by default
    if (total == INVALID_LEN)
        total = reservedPos > 0 ? logSegmentSize : mainSegmentSize;

    //debug_info("Scan bytes from segment %d total %lld\n", Segment::getId(segment), total);
    while (bytesScanned < (size_t) total) {
        bytesScanned = gcKvPair(groupId, &segment, bytesScanned, keyCount, isRemove, reservedPos, gcMode, validBytes);
    }

    return bytesScanned;
}

segment_len_t GCManager::gcKvPair(group_id_t groupId, Segment *segment, segment_len_t scanned, std::unordered_map<unsigned char *, std::pair<int, ValueLocation>, hashKey, equalKey> &keyCount, bool isRemove, size_t reservedPos, int gcMode, size_t *validBytes) {

    std::pair<unsigned char *, std::pair<int, ValueLocation>> kc; 
    len_t valueSize = INVALID_LEN;
    off_len_t offLen;
    segment_len_t mainSegmentSize = ConfigManager::getInstance().getMainSegmentSize();
    segment_len_t logSegmentSize = ConfigManager::getInstance().getLogSegmentSize();

    // find the key's position
    kc.first = Segment::getData(*segment);
    kc.first += scanned;

    // skip paddings for GC in-memory updates
    if (*kc.first == 0) {
        return ++scanned;
    }

    //printf("Scan key (%d)[%x] at %lu v[0] = %x\n", KEY_SIZE, kc.first[0], scanned, kc.first[sizeof(len_t)+KEY_SIZE]);
    kc.second.first = 1;
    ValueLocation valueLoc;
    valueLoc.segmentId = Segment::getId(*segment);
    valueLoc.offset = scanned + (reservedPos > 0? mainSegmentSize + (reservedPos - 1) * logSegmentSize : 0);
    kc.second.second = valueLoc;

    // skip the value and value size / deleted keys 
    offLen.first = scanned + KEY_SIZE;
    offLen.second = sizeof(len_t);
    Segment::readData(*segment, &valueSize, offLen);
    kc.second.second.length = valueSize;

    // deleted key is always assumed to be deleted
    // Todo handle ordering of delete when update are distributed in a group
    auto it = keyCount.find(kc.first);
    if (it != keyCount.end()) {
        kc.second.first += it->second.first;
        keyCount.erase(it);
    }
    scanned += KEY_SIZE;

    //debug_info("Scan value of size %lld\n", valueSize);
    if (valueSize == INVALID_LEN) {
        // key deleted
        kc.second.first = -1;
        //debug_info("Scan deleted key [%.*s]\n", KEY_SIZE, kc.first);
    // Todo temp workaround for filtering invalid values
    } else if (valueSize > 0) {
        // new key scanned
        if (kc.second.first == 1 && validBytes != 0) {
            *validBytes += KEY_SIZE + sizeof(len_t) + valueSize;
        }
        //debug_info("Scanned [%.*s] with size %lld\n", KEY_SIZE, kc.first, valueSize);
    } else if (valueSize == 0) {
        if (validBytes != 0) {
            *validBytes += KEY_SIZE + sizeof(len_t);
        }
        kc.second.first += TAG_MASK;
    } else {
        // Todo handle invalid value sizes
        debug_error("ValueSize is %lu at offset %lu of contianer %lu\n", valueSize, scanned, Segment::getId(*segment));
        assert(0);
        exit(1);
    }
    // add/update in the key count map
    keyCount.insert(kc);

    // skip the key and value 
    scanned += sizeof(len_t) + (valueSize == INVALID_LEN? 0 : valueSize);

    return scanned;
}

inline int GCManager::getHotness(group_id_t groupId, int updateCount) {
    return (updateCount > 1)? ConfigManager::getInstance().getHotnessLevel() / 2 : 0;
}

GCMode GCManager::getGCMode(group_id_t groupId, len_t reservedBytes) {
    GCMode gcMode = ConfigManager::getInstance().getGCMode();
    // check if the triggering condition(s) matches,
    // swtich to all if 
    // (1) log reclaim is less than reclaim min threshold
    // (2) write back ratio is smaller for all
    double ratioAll = _segmentGroupManager->getGroupWriteBackRatio(groupId, /* type = */ 0, /* isGC = */ true);
    double ratioLogOnly = _segmentGroupManager->getGroupWriteBackRatio(groupId, /* type = */ 1, /* isGC = */ true);
    if (gcMode == LOG_ONLY && ratioAll <= ratioLogOnly) {
        gcMode = ALL;
    }
    return gcMode;
}

#undef TAG_MASK
