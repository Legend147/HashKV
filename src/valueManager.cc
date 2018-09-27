#include "valueManager.hh"
#include "util/timer.hh"

#define RECORD_SIZE     ((valueSize == INVALID_LEN? 0 : valueSize) + (LL)sizeof(len_t) + KEY_SIZE)
#define RECOVERY_MULTI_THREAD_ENCODE
#define NUM_RESERVED_GC_SEGMENT    (2)

// always reserve (at least) one spare for flush of reserved space
int ValueManager::spare = 1;

ValueManager::ValueManager(DeviceManager *deviceManager, SegmentGroupManager *segmentGroupManager, KeyManager *keyManager, LogManager *logManager, bool isSlave) {
    // init the connections to different mods
    _deviceManager = deviceManager;
    _segmentGroupManager = segmentGroupManager;
    _keyManager = keyManager;
    //_gcManager = 0;
    _logManager = logManager;
    _slaveValueManager = 0;

    ConfigManager &cm = ConfigManager::getInstance();

    _isSlave = isSlave;

    if (!isSlave && cm.useSlave()) {
        std::vector<DiskInfo> disks;
        if (cm.useSeparateColdStorageDevice()) {
            DiskInfo coldDisk(0, cm.getColdStorageDevice().c_str(), cm.getColdStorageCapacity());
            disks.push_back(coldDisk);
        } else {
            disks = deviceManager->getDisks();
            // Todo: array segment alignment for mulitple disks
            for (unsigned int i = 0; i < disks.size(); i++) {
                disks.at(i).skipOffset = cm.getMainSegmentSize() * cm.getNumMainSegment() + cm.getLogSegmentSize() * cm.getNumLogSegment();
            }
        }
        _slave.dm = new DeviceManager(disks, /* isSlave = */ true);
        _slave.cgm = new SegmentGroupManager(/* isSlave = */ true, _keyManager);
        _slaveValueManager = new ValueManager(_slave.dm, _slave.cgm, keyManager, 0, /* isSlave = */ true);
        _slave.gcm = new GCManager(keyManager, _slaveValueManager, _slave.dm, _slave.cgm, /* isSlave = */ true);
        _slaveValueManager->setGCManager(_slave.gcm);
        //printf("dm %p cgm %p svm %p\n", _slave.dm, _slave.cgm, _slaveValueManager);
    } else {
        _slave.dm = 0;
        _slave.cgm = 0;
        _slave.gcm = 0;
    }
    _slave.writtenBytes = _segmentGroupManager->getLogWrittenBytes();
    _slave.validBytes = _segmentGroupManager->getLogValidBytes();

    // always use vlog by default for slave
    bool vlogEnabled = _isSlave? true : cm.enabledVLogMode();

    // level of hotness
    int hotnessLevel = cm.getHotnessLevel();
    if (vlogEnabled) {
        hotnessLevel = 1;
    }

    // one coding for diff hotness
    _activeSegments.resize(hotnessLevel + spare);
    _curSegmentBuffer.resize(hotnessLevel);
    // pre-allocate k segments for each hotness level
    _segmentBuffers.resize(hotnessLevel);

    // init lock for spare buffers
    for (int i = 0; i < spare; i++) {
        // only init the record, leave the data to be mapped to reserved space pool on flush
        INIT_LIST_HEAD(&_activeSegments.at(i+hotnessLevel));
    }

    int numPipelinedBuffer = cm.getNumPipelinedBuffer();
    // abstract the pool as a segment
    // pool(s) for updates
    for (int i = 0; i < numPipelinedBuffer; i++) {
        _centralizedReservedPool[i].size = _isSlave? cm.getMainSegmentSize() : cm.getUpdateKVBufferSize();
        if (_centralizedReservedPool[i].size <= cm.getLogSegmentSize())
            _centralizedReservedPool[i].size = cm.getLogSegmentSize();
        Segment::init(_centralizedReservedPool[i].pool, INVALID_SEGMENT, _centralizedReservedPool[i].size);
    }
    _centralizedReservedPoolIndex.flushNext = 0;
    _centralizedReservedPoolIndex.inUsed = 0;

    // special pool for log segments only flush
    Segment::init(_centralizedReservedPool[numPipelinedBuffer].pool, INVALID_SEGMENT, cm.getMainSegmentSize()*2);

    // init spare reserved space buffers for flush
    _segmentReservedPool = new SegmentPool(cm.getNumParallelFlush() + NUM_RESERVED_GC_SEGMENT, SegmentPool::poolType::log);

    // segment of zeros
    Segment::init(_zeroSegment, INVALID_SEGMENT, cm.getMainSegmentSize());

    // segment for read
    Segment::init(_readBuffer, INVALID_SEGMENT, cm.getMainSegmentSize());

    // thread pool for I/Os
    _iothreads.size_controller().resize(cm.getNumIOThread());
    _flushthreads.size_controller().resize(cm.getNumParallelFlush());

    // thread for background flush
    _started = true;
    pthread_mutex_init(&_centralizedReservedPoolIndex.queueLock, 0);
    int ret = 0;
    pthread_attr_t attr;
    ret = pthread_attr_init(&attr);
    if (ret != 0) {
        debug_error("failed to init bg flush thread attr (%d)\n", ret);
        assert(0);
        exit(-1);
    }
    ret = pthread_cond_init(&_needBgFlush, 0);
    ret = pthread_cond_init(&_centralizedReservedPoolIndex.flushedBuffer, 0);
    if (ret != 0) {
        debug_error("failed to init bg flush cond (%d)\n", ret);
        assert(0);
        exit(-1);
    }
    ret = pthread_create(&_bgflushThread, &attr, &flushCentralizedReservedPoolBgWorker, (void *) this);
    if (ret != 0) {
        debug_error("failed to init bg flush thread (%d)\n", ret);
        assert(0);
        exit(-1);
    }

    // restore from any log after failure
    if (_logManager) {
        restoreFromUpdateLog();
        restoreFromGCLog();
    }

}

ValueManager::~ValueManager() {
    // flush and release segments
    //forceSync(true, true);

    // allow the bg thread to finish its job first
    _started = false;
    pthread_cond_signal(&_needBgFlush);
    pthread_join(_bgflushThread, 0);
    
    ConfigManager &cm = ConfigManager::getInstance();

    // release buffers
    int hotnessLevel = cm.getHotnessLevel();
    if (cm.enabledVLogMode() || _isSlave) {
        hotnessLevel = 1;
    }
    list_head *ptr;
    // spare reserved buffers 
    for (int i = 0; i < spare; i++) {
        list_for_each(ptr, &_activeSegments[hotnessLevel + i]) {
            Segment::free(segment_of(ptr, SegmentBuffer, node)->segment);
        }
    }
    // centralized reserved pool
    for (int i = 0; i < cm.getNumPipelinedBuffer(); i++) {
        Segment::free(_centralizedReservedPool[i].pool);
    }
    // reserved pool
    assert (_segmentReservedPool->getUsage().second == 0);
    delete _segmentReservedPool;
    // segment of zeros
    Segment::free(_zeroSegment);
    Segment::free(_readBuffer);

    delete _slaveValueManager;
    delete _slave.gcm;
    delete _slave.cgm;
    delete _slave.dm;
}

ValueLocation ValueManager::putValue (char *keyStr, len_t keySize, char *valueStr, len_t valueSize, const ValueLocation &oldValueLoc, int hotness) {
    ValueLocation valueLoc;

    segment_id_t segmentId = oldValueLoc.segmentId;

    // debug message for deleting key
    if (valueSize == INVALID_LEN) {
        debug_info("DELETE: [%.*s]\n", KEY_SIZE, keyStr);
    }

    // avoid GC
    _GCLock.lock();
    volatile int &poolIndex = _centralizedReservedPoolIndex.inUsed;
    unsigned char *key = (unsigned char*) keyStr;
    bool vlog = _isSlave || ConfigManager::getInstance().enabledVLogMode();

    // convert to reference to main group
    ValueLocation convertedLoc = oldValueLoc;

    // check if group is GCed
    if (oldValueLoc.segmentId != LSM_SEGMENT /* not in LSM */ && _segmentGroupManager->getGroupBySegmentId(segmentId) == INVALID_GROUP /* group not found */ && !vlog) {
        _GCLock.unlock();
        return valueLoc;
    }
    // retain updates to a segment if it is in buffer
    group_id_t groupId = INVALID_GROUP;
    if (vlog) {
        groupId = 0;
        convertedLoc.segmentId = oldValueLoc.segmentId;
    } else if (oldValueLoc.segmentId != LSM_SEGMENT) { // not key-value in LSM-tree
        while(!_segmentGroupManager->convertRefMainSegment(convertedLoc, true));
        // locate the group
        groupId = _segmentGroupManager->getGroupBySegmentId(convertedLoc.segmentId);
        assert(groupId != INVALID_GROUP);
    } else {
        groupId = LSM_GROUP;
    }

    // check if in-place update is possible
    bool inPlaceUpdate = false;
    bool inPool = false;
    len_t oldValueSize = INVALID_LEN;

    std::unordered_map<unsigned char*, segment_len_t, hashKey, equalKey>::iterator keyIt = _centralizedReservedPool[poolIndex].keysInPool.find(key);
    inPool = keyIt != _centralizedReservedPool[poolIndex].keysInPool.end();

    if (inPool) {
        // always remove out-dated position of the key
        _centralizedReservedPool[poolIndex].keysInPool.erase(keyIt);

        // consider in-place update if size is the same (and such update is allowed)
        if (ConfigManager::getInstance().isInPlaceUpdate()) {
            off_len_t offLen (keyIt->second + KEY_SIZE, sizeof(len_t));
            Segment::readData(_centralizedReservedPool[poolIndex].pool, &oldValueSize, offLen);
            assert(oldValueSize == INVALID_LEN || oldValueSize > 0);
            if (oldValueSize == INVALID_LEN) {
                oldValueSize = 0;
            }
            inPlaceUpdate = oldValueSize == valueSize;
        }
    }

    // flush before write, if no more place for updates
    if (!Segment::canFit(_centralizedReservedPool[poolIndex].pool, RECORD_SIZE) && !inPlaceUpdate) {
        _GCLock.unlock();
        group_id_t prevGroupId = groupId;
        //printf("Flush before write\n");
        if (ConfigManager::getInstance().usePipelinedBuffer()) {
            flushCentralizedReservedPoolBg(StatsType::POOL_FLUSH);
        } else if (vlog) {
            STAT_TIME_PROCESS(flushCentralizedReservedPoolVLog(poolIndex), StatsType::POOL_FLUSH);
        } else {
            STAT_TIME_PROCESS(flushCentralizedReservedPool(&groupId, /* isUpdate = */ true), StatsType::POOL_FLUSH);
        }
        // need the kvserver to search if the location of key may be updated
        if (groupId == INVALID_GROUP && prevGroupId != LSM_GROUP /* not key-values in LSM */) {
            _segmentGroupManager->releaseGroupLock(prevGroupId);
            return valueLoc;
        }
        _GCLock.lock();
        inPool = false;
        inPlaceUpdate = false;
    }

    Segment &pool = _centralizedReservedPool[poolIndex].pool;

    if (groupId != LSM_GROUP && !vlog) {
        // update group id of key-values not in LSM just in case of GC
        groupId = _segmentGroupManager->getGroupBySegmentId(convertedLoc.segmentId);
        if (groupId == INVALID_GROUP) {
            _GCLock.unlock();
            return valueLoc;
        }
    }

    // update the value (in-place or append to buffer)
    segment_len_t poolOffset = Segment::getWriteFront(pool);

    if (inPlaceUpdate) {
        // overwrite the value in-place
        poolOffset = keyIt->second;
        off_len_t offLen (poolOffset + KEY_SIZE + sizeof(len_t), valueSize);
        Segment::overwriteData(pool, valueStr, offLen);
        debug_info("Inplace update segment %lu len %lu\n", convertedLoc.segmentId, valueSize);

    } else {
        // append if new / cannot fit in-place
        Segment::appendData(pool, keyStr, KEY_SIZE);
        Segment::appendData(pool, &valueSize, sizeof(len_t));
        if (valueSize > 0) 
            Segment::appendData(pool, valueStr, valueSize);

        debug_info("append update to segment %lu len %lu\n", convertedLoc.segmentId, valueSize);
        // increment the total update size counter for the segment
        _centralizedReservedPool[poolIndex].segmentsInPool[convertedLoc.segmentId].first += RECORD_SIZE;
        // add the offset (pointer) for the segment
        _centralizedReservedPool[poolIndex].segmentsInPool[convertedLoc.segmentId].second.insert(poolOffset);
        // mark the present of group in pool
        _centralizedReservedPool[poolIndex].groupsInPool[groupId].insert(RECORD_SIZE);
    }

    // add the updated key offset
    std::pair<unsigned char*, segment_len_t> keyOffset (Segment::getData(pool) + poolOffset, poolOffset);
    _centralizedReservedPool[poolIndex].keysInPool.insert(keyOffset);

    if (groupId != LSM_GROUP && !vlog) _segmentGroupManager->releaseGroupLock(groupId);

    // flush after write, if the pool is (too) full
    if (!Segment::canFit(pool, 1) || ConfigManager::getInstance().getUpdateKVBufferSize() <= 0) {
        _GCLock.unlock();
        if (ConfigManager::getInstance().usePipelinedBuffer()) {
            flushCentralizedReservedPoolBg(StatsType::POOL_FLUSH);
        } else if (vlog) {
            STAT_TIME_PROCESS(flushCentralizedReservedPoolVLog(poolIndex), StatsType::POOL_FLUSH);
        } else {
            STAT_TIME_PROCESS(flushCentralizedReservedPool(/* reportGroupId* = */ 0, /* isUpdate = */ true), StatsType::POOL_FLUSH);
        }
    } else {
        _GCLock.unlock();
    }

    valueLoc.segmentId = oldValueLoc.segmentId;
    valueLoc.length = valueSize + (groupId != LSM_GROUP? sizeof(len_t) : 0);

    // use the old value location as a hints for GET before flush
    return valueLoc;
}

bool ValueManager::getValueFromBuffer (const char *keyStr, char *&valueStr, len_t &valueSize) {

    unsigned char *key = (unsigned char*) keyStr;

    // always look into write buffer first
    // Todo (?) wait for bg flush to complete (metadata to settle)
    for (int idx = _centralizedReservedPoolIndex.inUsed; 1 ; decrementPoolIndex(idx)) {
        _centralizedReservedPool[idx].lock.lock();
        auto it = _centralizedReservedPool[idx].keysInPool.find(key);
        if (it != _centralizedReservedPool[idx].keysInPool.end()) {
            segment_len_t start = it->second;
            off_len_t offLen(start + KEY_SIZE, sizeof(len_t));
            Segment::readData(_centralizedReservedPool[idx].pool, &valueSize, offLen);
            assert(valueSize > 0 || valueSize == INVALID_LEN);
            if (valueSize > 0) {
                //printf("Read update buf offset %lu\n", it->second);
                offLen = {start + KEY_SIZE + sizeof(len_t), valueSize};
                valueStr = (char*) buf_malloc (valueSize);
                Segment::readData(_centralizedReservedPool[idx].pool, valueStr, offLen);
            }
            _centralizedReservedPool[idx].lock.unlock();
            return true;
        }
        _centralizedReservedPool[idx].lock.unlock();
        // loop at least one time to check the buffer in-use, if queue is empty, there is no more buffer to check, so skip looping all yet flushed buffers
        if (idx == _centralizedReservedPoolIndex.flushNext && _centralizedReservedPoolIndex.queue.empty())
            break;
    }

    return false;
}

bool ValueManager::getValueFromDisk (const char *keyStr, ValueLocation readValueLoc, char *&valueStr, len_t &valueSize) {

    ConfigManager &cm = ConfigManager::getInstance();
    bool vlog = _isSlave || cm.enabledVLogMode();
    bool ret = false;
    Segment readBuffer;

    // Todo degraded read from device

    if (cm.useSlave() && readValueLoc.segmentId == cm.getNumSegment() && !_isSlave) {
        // access to slave
        ret = _slaveValueManager->getValueFromBuffer(keyStr, valueStr, valueSize);
        if (!ret) {
            ret = _slaveValueManager->getValueFromDisk(keyStr, readValueLoc, valueStr, valueSize);
        }
    } else if (vlog || _isSlave) {
        // vlog / slave mode
        valueStr = (char*) buf_malloc (KEY_SIZE + sizeof(len_t) + readValueLoc.length);
        if (_isSlave && cm.segmentAsFile() && cm.segmentAsSeparateFile()) {
            _deviceManager->readDisk(cm.getNumSegment(), (unsigned char *) valueStr, readValueLoc.offset, readValueLoc.length + KEY_SIZE + sizeof(len_t));
        } else {
            _deviceManager->readDisk(/* diskId = */ 0, (unsigned char *) valueStr, readValueLoc.offset, readValueLoc.length + KEY_SIZE + sizeof(len_t));
        }
        off_len_t offLen = {KEY_SIZE, sizeof(len_t)};
#ifndef NDEBUG
        memcpy(&valueSize, valueStr + KEY_SIZE, sizeof(len_t));
#else
        valueSize = readValueLoc.length;
#endif //NDEBUG
        assert(valueSize == readValueLoc.length);
        offLen = {sizeof(len_t) + KEY_SIZE, valueSize};
        assert(memcmp(valueStr, keyStr, KEY_SIZE) == 0);
        memmove(valueStr, valueStr + KEY_SIZE + sizeof(len_t), valueSize);
        //printf("Read disk offset %lu length %lu %x\n", readValueLoc.offset, offLen.second, valueStr[0]);
        ret = true;
    } else {
        // read data
        valueStr = (char*) buf_malloc (readValueLoc.length + KEY_SIZE);
        _deviceManager->readPartialSegment(readValueLoc.segmentId, readValueLoc.offset, readValueLoc.length + KEY_SIZE, (unsigned char *)valueStr);
        // check and read value size 
#ifndef NDEBUG
        memcpy(&valueSize, valueStr + KEY_SIZE, sizeof(len_t));
#else
        valueSize = readValueLoc.length - sizeof(len_t);
#endif // NDEBUG
        assert(memcmp(valueStr, keyStr, KEY_SIZE) == 0);
        assert(valueSize + sizeof(len_t) == readValueLoc.length);
        // adjust value position in buffer
        memmove(valueStr, valueStr + KEY_SIZE + sizeof(len_t), valueSize);
        //printf("Read disk group %lu segment %lu offset %lu length %lu\n", _segmentGroupManager->getGroupBySegmentId(readValueLoc.segmentId), readValueLoc.segmentId, readValueLoc.offset, readValueLoc.length);
        ret = true;
    }

    return ret;
}

// caller should lock the centralized reserved pool
bool ValueManager::prepareGCGroupInCentralizedPool(group_id_t groupId, bool needsLock) {
    return setGroupReservedBufferCP(groupId, needsLock, true, groupId);
}

bool ValueManager::setGroupReservedBufferCP (segment_id_t mainSegmentId, bool needsLock, bool isGC, segment_id_t logSegmentId, bool groupMetaOutDated, std::unordered_map<std::pair<segment_id_t,segment_id_t>, len_t, hashCidPair> *invalidBytes, int poolIndex) {
    SegmentBuffer *cb = 0;
    segment_id_t segmentId = 0;
    bool done = false;

    group_id_t groupId = _segmentGroupManager->getGroupBySegmentId(mainSegmentId);
    segment_len_t mainSegmentSize = ConfigManager::getInstance().getMainSegmentSize();

    // copy updates back to segment buffers in group for flush
    
    if (groupId == INVALID_GROUP) {
        debug_error("Need main group to associate reserved segments of group %lu\n", groupId);
        assert(0);
        exit(-1);
    }

    //_segmentReservedSetLock.lock();

    if (isGC) debug_info("GC group %lu in centralized Pool\n", groupId);

    int numBufferToScan = 1;
    if (ConfigManager::getInstance().usePipelinedBuffer() && isGC) {
        numBufferToScan = ConfigManager::getInstance().getNumPipelinedBuffer(); //((_centralizedReservedPoolIndex.inUsed - poolIndex + numPipelinedBuffer + 1) % numPipelinedBuffer);
    }

    // pre-allocate all reserved space buffer in the group, if we distribute updates
    size_t updateTotal = 0;

    for (int idx = (isGC? _centralizedReservedPoolIndex.flushNext : poolIndex), cnt = numBufferToScan; cnt > 0; idx = getNextPoolIndex(idx), cnt--) {
        if (_centralizedReservedPool[idx].groupsInPool.count(groupId) == 0)
            continue;
        for (auto s : _centralizedReservedPool[idx].groupsInPool.at(groupId)) {
            updateTotal += s;
        }
    }

    // no updates at all ..
    if (updateTotal == 0) {
        //printf("[Group %lu] No updates isGC=%d\n", groupId, isGC);
        return true;
    } else if (isGC) {
        //printf("[Group %lu] updates %lu isGC=%d\n", groupId, updateTotal, isGC);
    }
        

    segmentId = logSegmentId;
    // get the buffer record
    cb = &_segmentReservedInBuf[segmentId];
    // determine the reserved space of the segment (min.: original size, max.: flush front + all updates in buffer)
    // Todo more accurate space allocation of buffers
    // get a segment buffer (with logSegmentId assigned) of necessary size
    segment_len_t logSegmentSize = ConfigManager::getInstance().getLogSegmentSize();
    segment_len_t poolSegmentSize = _segmentReservedPool->getPoolSegmentSize();
    segment_len_t needSegmentSize = poolSegmentSize;
    offset_t groupFlushFront = _segmentGroupManager->getGroupFlushFront(groupId, false);
    size_t existingTotal = (getLastSegmentFront(groupFlushFront) % logSegmentSize);
    if (existingTotal + updateTotal > poolSegmentSize && isGC) {
        needSegmentSize = existingTotal + updateTotal;
    } else if (mainSegmentId == logSegmentId) {
        needSegmentSize = mainSegmentSize;
    }
    cb->segment = _segmentReservedPool->allocSegment(logSegmentId, &cb->lock, needSegmentSize);
    // check if the allocation of segment buffer is successful
    if (Segment::getId(cb->segment) == INVALID_SEGMENT) {
        debug_error("Out of reserved space segment from pool for segment %lu\n", segmentId);
        //_segmentReservedSetLock.unlock();
        return false;
    }
    //Segment::assignId(cb->segment, logSegmentId);
    // set data flush front
    Segment::setFlushFront(cb->segment, groupMetaOutDated? 0 : (mainSegmentId == logSegmentId && !isGC? groupFlushFront : (getLastSegmentFront(groupFlushFront) % logSegmentSize)));
    // if group in new write buffer not yet flushed
    if (_segmentReservedByGroup.count(groupId) == 0) {
        INIT_LIST_HEAD(&_segmentReservedByGroup[groupId]);
    } else if (ConfigManager::getInstance().getNumParallelFlush() == 1) {
        assert(0);
    }
    INIT_LIST_HEAD(&cb->node);
    list_add(&cb->node, &_segmentReservedByGroup.at(groupId));

    // assume all updates can fit in first
    done = true;
    segmentId = mainSegmentId;

    std::vector<segment_id_t> logSegments = _segmentGroupManager->getGroupLogSegments(groupId, false);
    len_t valueSize = 0;
    // copy and align all updates to the segment buffer (reserved space)
    // Todo merge the scanning here with that during GC?
    size_t osize = 0;
    std::unordered_set<len_t> invalidOffsetSet;
    for (int idx = (isGC? _centralizedReservedPoolIndex.flushNext : poolIndex), cnt = numBufferToScan; cnt > 0; idx = getNextPoolIndex(idx), cnt--) {
        if (_centralizedReservedPool[idx].segmentsInPool.count(segmentId) == 0)
            continue;
        unsigned char *poolData = Segment::getData(_centralizedReservedPool[idx].pool);
        osize += _centralizedReservedPool[idx].segmentsInPool.at(segmentId).second.size();
        for (auto update = _centralizedReservedPool[idx].segmentsInPool.at(segmentId).second.begin();
                update != _centralizedReservedPool[idx].segmentsInPool.at(segmentId).second.end();
                update = _centralizedReservedPool[idx].segmentsInPool.at(segmentId).second.begin()
        ) {
            // choose the target buffer if we distribute updates
            off_len_t offLen (*update + KEY_SIZE, sizeof(len_t));
            Segment::readData(_centralizedReservedPool[idx].pool, &valueSize, offLen);
            //assert((valueSize > 0 && valueSize <= segmentSize - RECORD_SIZE) || valueSize == INVALID_LEN);
            // always place all data into segment for GC, but not flush
            if (Segment::canFit(cb->segment, RECORD_SIZE) == false) {
                if (isGC == false) {
                    // leave updates to next reserved segment if cannot fit in
                    // (if cannot fit in least-free segment, will not fit in any others)
                    break;
                } else {
                    // all updates must fit in for GC
                    assert(0);
                }
            }
            // copy data from centralized buffer to (log) segment buffer
            Segment::appendData(cb->segment, poolData + *update, KEY_SIZE);
            Segment::appendData(cb->segment, &valueSize, sizeof(len_t));
            if (valueSize > 0)
                Segment::appendData(cb->segment, poolData + *update + KEY_SIZE + sizeof(len_t), valueSize);
            // remove to ensure no duplicated flush of same update
            _centralizedReservedPool[idx].segmentsInPool.at(segmentId).second.erase(update);
        }
        // check if all updates are fit for the current data segment
        done = _centralizedReservedPool[idx].segmentsInPool.at(segmentId).second.empty() && done;
    }

    if (isGC && !done) {
        debug_error("isGC for segment %lu cannot fit in %lu of %lu updates\n", segmentId, _centralizedReservedPool[poolIndex].segmentsInPool.at(segmentId).second.size(), osize);
        assert(0);
    }

    //_segmentReservedSetLock.unlock();

    return done;
}

bool ValueManager::cleanupGCGroupInCentralizedPool(group_id_t groupId, bool isGCDone, bool needsLockPool) {
    return releaseGroupReservedBufferCP(groupId, needsLockPool, /* isGC = */ true, isGCDone);
}

bool ValueManager::releaseGroupReservedBufferCP(group_id_t groupId, bool needsLockPool, bool isGC, bool isGCDone, int poolIndex) {
    // release the updates in the reserved pool
    if (!isGCDone) {
        return false;
    }

    int numBufferToRelease = 1;
    if (ConfigManager::getInstance().usePipelinedBuffer() && isGC) {
        numBufferToRelease = ConfigManager::getInstance().getNumPipelinedBuffer(); //((_centralizedReservedPoolIndex.inUsed - poolIndex + numPipelinedBuffer) % numPipelinedBuffer) + 1;
    }

    for (int idx = (isGC? _centralizedReservedPoolIndex.flushNext : poolIndex), cnt = numBufferToRelease; cnt > 0; idx = getNextPoolIndex(idx), cnt--) {
        if (needsLockPool) _centralizedReservedPool[idx].lock.lock();
        segment_id_t mainSegmentId = _segmentGroupManager->getGroupMainSegment(groupId);
        if (_centralizedReservedPool[idx].segmentsInPool.count(mainSegmentId) == 0 ||
                _centralizedReservedPool[idx].segmentsInPool.at(mainSegmentId).second.empty()) {
            _centralizedReservedPool[idx].segmentsInPool.erase(mainSegmentId);
        }
        // GC is group-based, flush is batch of groups and leave to caller to free all at once
        if (isGC)
            _centralizedReservedPool[idx].groupsInPool.erase(groupId);
        if (needsLockPool) _centralizedReservedPool[idx].lock.unlock();
    }

    // release buffers
    if (_segmentReservedByGroup.count(groupId)) {
        list_head *ptr, *sptr;
        if (!list_empty(&_segmentReservedByGroup.at(groupId))) {
            list_for_each_safe(ptr, sptr, &_segmentReservedByGroup.at(groupId)) {
                SegmentBuffer *cb = segment_of(ptr, SegmentBuffer, node);
                segment_id_t logSegmentId = Segment::getId(cb->segment);
                _segmentReservedPool->releaseSegment(logSegmentId);
                // only release segments with reserved space (not main segments)
                if (_segmentReservedInBuf.count(logSegmentId) > 0) {
                    list_del(&_segmentReservedInBuf.at(logSegmentId).node);
                    _segmentReservedInBuf.erase(logSegmentId);
                }
            }
        }
        // release group
        _segmentReservedByGroup.erase(groupId);
    }

    return true;
}

bool ValueManager::outOfReservedSpace(offset_t flushFront, group_id_t groupId, int poolIndex) {
    len_t mainSegmentSize = ConfigManager::getInstance().getMainSegmentSize();
    len_t logSegmentSize = ConfigManager::getInstance().getLogSegmentSize();
    segment_id_t numReserved = ConfigManager::getInstance().getNumPipelinedBuffer() - 1;
    bool oors = false;

    // for reserved segments, the max amount that can be write is (free groups * segment size)
    if (_segmentGroupManager->getNumFreeLogSegments() <= MIN_FREE_SEGMENTS + numReserved) {
        oors = true;
        return oors;
    }
    len_t logSegmentSpace = (len_t) (_segmentGroupManager->getNumFreeLogSegments() - (MIN_FREE_SEGMENTS + numReserved)) * logSegmentSize;

    // see if the sum of updates exceed the log space remains
    len_t sum = 0;
    if (flushFront > mainSegmentSize) {
        sum = (flushFront - mainSegmentSize) % logSegmentSize;
    }

    // pool is empty
    if (_centralizedReservedPool[poolIndex].segmentsInPool.empty()) {
        return oors;
    }

    // find the remaining log space
    for (auto u : _centralizedReservedPool[poolIndex].segmentsInPool.at(_segmentGroupManager->getGroupMainSegment(groupId)).second) {
        len_t valueSize = 0;
        off_len_t offLen (u + KEY_SIZE, sizeof(len_t));
        Segment::readData(_centralizedReservedPool[poolIndex].pool, &valueSize, offLen);
        if (sum + RECORD_SIZE > logSegmentSpace) {
            oors = true;
            //printf("out of reserved for segment %d\n", cid);
            break;
        }
        sum += RECORD_SIZE;
    }

    return oors;
}

bool ValueManager::outOfReservedSpaceForObject(offset_t flushFront, len_t objectSize) {
    ConfigManager &cm = ConfigManager::getInstance();
    len_t mainSegmentSize = cm.getMainSegmentSize();
    segment_id_t numReserved = cm.getNumPipelinedBuffer() - 1;
    bool toMain = flushFront < mainSegmentSize;

    // whether the object fits into main segment
    if (toMain && flushFront + objectSize <= mainSegmentSize) {
        return false;
    }

    // for reserved segments, the max amount that can be write is (free groups * segment size)
    if (_segmentGroupManager->getNumFreeLogSegments() <= MIN_FREE_SEGMENTS + numReserved) {
        return true;
    }

    return false;
}

// caller should lock _centralizedReservedPool before-hand
void ValueManager::flushCentralizedReservedPool (group_id_t *reportGroupId, bool isUpdate, int poolIndex, std::unordered_map<unsigned char*, offset_t, hashKey, equalKey> *oldLocations) {

    const int flushingPoolIndex = poolIndex;
    std::lock_guard<std::mutex> (_centralizedReservedPool[poolIndex].lock);

    //printf("Flush centralized reserved pool %d with %lu groups %lu segments\n", flushingPoolIndex, _centralizedReservedPool[flushingPoolIndex].groupsInPool.size(), _centralizedReservedPool[flushingPoolIndex].segmentsInPool.size());
    //int hotnessLevel = ConfigManager::getInstance().getHotnessLevel();
    segment_len_t logSegmentSize = ConfigManager::getInstance().getLogSegmentSize();
    segment_len_t mainSegmentSize = ConfigManager::getInstance().getMainSegmentSize();
    // mark which group is in-process, and the group of the upcominig segment
    // use the last (few?) buffers for flush
    //int bufferLevel = hotnessLevel + spare - 1;

    // Todo check the sequence of locks

    // for batch update
    std::unordered_map<segment_id_t, offset_t> metaToUpdate; // group id -> (flush fronts)
    std::vector<char *> keys, delKeys;
    std::vector<ValueLocation> values, delValues;
    std::atomic<int> waitIO;
    offset_t writeOffset = 0;

    std::unordered_map<group_id_t, unsigned long long> updateCounts;
    std::vector<Segment> tmpSegments;
    
    waitIO = 0;
    writeOffset = INVALID_OFFSET;

    //bool done = false;
    //bool newReservedStripe = false;
    group_id_t groupId = INVALID_GROUP;
    //group_id_t prevGroupId = INVALID_GROUP;
    segment_id_t mainSegmentId = INVALID_SEGMENT;
    segment_id_t logSegmentId = INVALID_SEGMENT;
    struct timeval flushStartTime, keyWriteStartTime;
    //len_t flushFront = 0;

    len_t batchWriteThreshold = ConfigManager::getInstance().getBatchWriteThreshold(); // write as 4KB chunks
    int numPipelinedBuffer = ConfigManager::getInstance().getNumPipelinedBuffer();
    bool isGCLogOnlyBuffer = flushingPoolIndex == numPipelinedBuffer;
    unsigned char *poolData = Segment::getData(_centralizedReservedPool[flushingPoolIndex].pool);

    bool gcCrashConsistency = !isUpdate && ConfigManager::getInstance().enableCrashConsistency();

    Segment segment;
    offset_t inSegmentOffset = 0;

#define RESET_SEGMENT_BUFFER() do { \
        Segment::init(segment, INVALID_SEGMENT, (unsigned char*) 0, INVALID_LEN); \
    } while (0)

#define FLUSH_SEGMENT_BUFFER(_THRD_) do { \
        if (Segment::getWriteFront(segment) - Segment::getFlushFront(segment) > _THRD_) { \
            waitIO += 1; \
            _iothreads.schedule( \
                    std::bind( \
                        &DeviceManager::writePartialSegmentMt, \
                        _deviceManager, \
                        Segment::getId(segment), \
                        Segment::getFlushFront(segment) + inSegmentOffset, \
                        Segment::getWriteFront(segment) - Segment::getFlushFront(segment), \
                        Segment::getData(segment) + Segment::getFlushFront(segment), \
                        boost::ref(writeOffset), \
                        boost::ref(waitIO) \
                    ) \
            ); \
            /* reset the buffer */\
            RESET_SEGMENT_BUFFER(); \
        } \
    } while(0)

    gettimeofday(&flushStartTime, 0);

    std::set<group_id_t> modifiedGroups;
    std::set<segment_id_t> modifiedSegments;

    for (auto group = _centralizedReservedPool[flushingPoolIndex].groupsInPool.begin();
            !_centralizedReservedPool[flushingPoolIndex].groupsInPool.empty();
            group = _centralizedReservedPool[flushingPoolIndex].groupsInPool.begin()
    ) {

        groupId = group->first;
        
        len_t valueSize = INVALID_LEN;

        bool toLSM = groupId == LSM_GROUP;
        if (toLSM) {
            mainSegmentId = LSM_SEGMENT;
        } else {
            mainSegmentId = _segmentGroupManager->getGroupMainSegment(groupId);
            modifiedGroups.insert(groupId);
        }

        // get a copy of the list of segments in group + purge the main segment -> list of log segments
        std::vector<segment_id_t> logSegments = _segmentGroupManager->getGroupLogSegments(groupId, false);

        offset_t flushFront = INVALID_OFFSET;
        // get the lastest flush front
        if (groupId != INVALID_GROUP && groupId != LSM_GROUP) flushFront = _segmentGroupManager->getGroupFlushFront(groupId, false);
        // iterator through the updates
        size_t numUpdates = _centralizedReservedPool[flushingPoolIndex].segmentsInPool.at(mainSegmentId).second.size();
        for (auto update = _centralizedReservedPool[flushingPoolIndex].segmentsInPool.at(mainSegmentId).second.begin();
                update != _centralizedReservedPool[flushingPoolIndex].segmentsInPool.at(mainSegmentId).second.end();
                update = _centralizedReservedPool[flushingPoolIndex].segmentsInPool.at(mainSegmentId).second.begin()
        ) {
            ValueLocation valueLoc;
            // read back the value size
            off_len_t offLen (*update + KEY_SIZE, sizeof(len_t));
            Segment::readData(_centralizedReservedPool[flushingPoolIndex].pool, &valueSize, offLen);
            valueLoc.length = valueSize + sizeof(len_t);
            assert(valueSize != INVALID_LEN && (valueSize != 0 || (isGCLogOnlyBuffer && ConfigManager::getInstance().useSlave()) ));

            bool isReservedOverflow = !isGCLogOnlyBuffer && outOfReservedSpaceForObject(flushFront, RECORD_SIZE);

            if (isReservedOverflow) {
                // let every out to disk first
                FLUSH_SEGMENT_BUFFER(0);
                while (waitIO > 0);
                assert(isUpdate);
                // consistency log
                if (ConfigManager::getInstance().enableCrashConsistency()) {
                    std::map<group_id_t, std::pair<offset_t, std::vector<segment_id_t> > > groups;
                    for (auto g : modifiedGroups) {
                        groups[g] = 
                                std::pair<offset_t, std::vector<segment_id_t> > (
                                        _segmentGroupManager->getGroupFlushFront(g, /* needsLock = */ false),
                                        _segmentGroupManager->getGroupSegments(g, /* needsLock = */ false)
                                );
                    }
                    _logManager->setBatchUpdateKeyValue(keys, values, groups);
                }
                // update LSM-tree
                gettimeofday(&keyWriteStartTime, 0);
                if (!keys.empty() && _keyManager->writeKeyBatch(keys, values) == false) {
                    debug_error("Failed to set %lu keys\n", keys.size());
                }
                StatsRecorder::getInstance()->timeProcess(isUpdate? StatsType::UPDATE_KEY_WRITE_LSM : StatsType::UPDATE_KEY_WRITE_LSM_GC, keyWriteStartTime, /* diff = */ 0, /* count = */ keys.size());
                // persist updates of the metadata to LSM-tree
                if (ConfigManager::getInstance().enableCrashConsistency()) {
                    // update persist log metadata
                    logMetaPersist(modifiedSegments, modifiedGroups);
                    modifiedGroups.clear();
                    modifiedSegments.clear();
                    // remove update consistency log
                    _logManager->ackBatchUpdateKeyValue();
                }

                // free all tmp buffers
                for (auto c : tmpSegments) {
                    Segment::free(c);
                }
                keys.clear();
                values.clear();
                tmpSegments.clear();
                // set the front global, so they are visible to GC
                _segmentGroupManager->setGroupFlushFront(groupId, flushFront, false);
                _segmentGroupManager->setGroupWriteFront(groupId, flushFront, false);

                // gc for some space
                do {
                    _gcManager->gcGreedy(/* needsGCLock = */ false, /* needsLockCP = */ false, reportGroupId);
                    // check if the update now fits into the free space
                    flushFront = _segmentGroupManager->getGroupFlushFront(groupId, false);
                    isReservedOverflow = !isGCLogOnlyBuffer && outOfReservedSpaceForObject(flushFront, RECORD_SIZE);
                } while (isReservedOverflow);

                break;
            }

            if (toLSM) { // write whole kv pair into LSM-tree
                valueLoc.segmentId = LSM_SEGMENT;
                valueLoc.length = valueSize;
                valueLoc.value.assign((char*) poolData + *update + KEY_SIZE + sizeof(len_t), valueSize);
            } else { // write key and location to LSM-tree, values to log

                // write frontier of the segment receiving the updates
                offset_t logSegmentFront = getLastSegmentFront(flushFront);

                // determine which segment to write the updates
                // priority for fitting updates 
                // (1) main segment if not sealed, 
                // (2) last log segment that is yet full if sealed
                // (3) new log segment
                if (logSegments.empty() /* no log segments */ &&
                    flushFront < mainSegmentSize /* main segment not sealed */ &&
                    valueSize + KEY_SIZE + sizeof(len_t) + flushFront <= mainSegmentSize /* all updates fit into the main segment */) {
                    // not yet full log group prompted as main group
                    // main segment is not yet full, and can receive updates
                    logSegmentId = mainSegmentId;
                } else if (flushFront >= mainSegmentSize /* main segment is sealed */ &&
                        logSegmentFront % logSegmentSize != 0 /* the last log segment is not yet full */ && 
                        valueSize + KEY_SIZE + sizeof(len_t) + logSegmentFront <= logSegmentSize
                        //_centralizedReservedPool[flushingPoolIndex].segmentsInPool.at(_segmentGroupManager->getGroupMainSegment(groupId)).first + logSegmentFront <= logSegmentSize /* all updates fit into the last log segment */
                        ) {
                    //assert(metaToUpdate.count(mainSegmentId) == 0);
                    // use reserved group allocated if not yet full, and will not overflow
                    logSegmentId = logSegments.back();
                } else {
                    // manual adjust the segment write and flush front to align the boundary
                    if (flushFront <= mainSegmentSize) {
                        flushFront = mainSegmentSize;
                    } else {
                        flushFront += (logSegmentSize - logSegmentFront);
                    }
                    // last log segment is full, allocate new a one to the group
                    logSegmentId = INVALID_SEGMENT;
                    if (!_segmentGroupManager->getNewLogSegment(groupId, logSegmentId, false)) {
                        debug_error("Out of log segments for group %lu? number of log %lu main %lu (isGCBuf = %d) flushFront = %lu RECORD_SIZE = %lu (%lu of %lu left)\n", groupId, _segmentGroupManager->getNumFreeLogSegments(), _segmentGroupManager->getNumFreeMainSegments(), isGCLogOnlyBuffer, flushFront, RECORD_SIZE, _centralizedReservedPool[flushingPoolIndex].segmentsInPool.at(mainSegmentId).second.size(), numUpdates);
                        assert(0);
                        exit(1);
                    }
                    assert(logSegmentId != INVALID_SEGMENT);
                    // keep track of the list of log segments locally
                    logSegments.push_back(logSegmentId);
                    // start from the begining of a new log segment
                    logSegmentFront = 0;
                }

                // setup the metadata
                valueLoc.segmentId = logSegmentId;
                valueLoc.offset = logSegmentFront;
                if (valueSize > 0) {
                    valueLoc.length = sizeof(len_t) + valueSize;
                } else {
                    valueLoc.length = 0;
                }


                if (valueSize > batchWriteThreshold && !gcCrashConsistency) {
                    // flush any data in the buffer first
                    FLUSH_SEGMENT_BUFFER(0);
                    // write individually
                    waitIO += 1;
                    _iothreads.schedule(
                            std::bind(
                                &DeviceManager::writePartialSegmentMt,
                                _deviceManager,
                                logSegmentId,
                                logSegmentFront,
                                RECORD_SIZE,
                                poolData + *update,
                                boost::ref(writeOffset),
                                boost::ref(waitIO)
                            )
                    );
                } else {
                    // write in-batch
                    // flush current buffer if target segment switched
                    if (Segment::getId(segment) != logSegmentId || !Segment::canFit(segment, RECORD_SIZE)) {
                        if (gcCrashConsistency) {
                            tmpSegments.push_back(segment);
                            RESET_SEGMENT_BUFFER();
                        } else {
                            FLUSH_SEGMENT_BUFFER(0);
                        }
                    }
                    // allocate a tmp buffer and track for later free
                    if (Segment::getId(segment) == INVALID_SEGMENT) {
                        Segment::init(
                                segment,
                                logSegmentId,
                                gcCrashConsistency? 
                                    (logSegmentId == mainSegmentId? mainSegmentSize : logSegmentSize) : 
                                    batchWriteThreshold * 2, 
                                /* set zero */ false
                        );
                        inSegmentOffset = logSegmentFront;
                        if (!gcCrashConsistency) {
                            tmpSegments.push_back(segment);
                        }
                    }
                    // append record to buffer
                    Segment::appendData(segment, poolData + *update, RECORD_SIZE);
                    if (!gcCrashConsistency) {
                        FLUSH_SEGMENT_BUFFER(batchWriteThreshold-1);
                    }

                }
                // keep track of the group flush front locally
                flushFront += RECORD_SIZE;
                modifiedSegments.insert(logSegmentId);
                // update the segment flush front
                _segmentGroupManager->setSegmentFlushFront(logSegmentId, logSegmentFront + RECORD_SIZE);
            }
            if (valueSize > 0) {
                // put it into the lists for batched put to LSM-tree
                // but skip tags
                keys.push_back((char*) poolData + *update);
                // save values first for gc consistency log
                if (gcCrashConsistency) {
                    valueLoc.value = std::string((char*) poolData + *update + sizeof(len_t), valueSize);
                }
                values.push_back(valueLoc);
            }
            _centralizedReservedPool[flushingPoolIndex].segmentsInPool.at(mainSegmentId).second.erase(update);

            // do not get the threadpool too busy
            //while (waitIO > ConfigManager::getInstance().getNumIOThread() * 1.5);
            
        }

        if (!gcCrashConsistency) {
            FLUSH_SEGMENT_BUFFER(0);
        }

        if (groupId != LSM_GROUP) {
            // update group metadata
            _segmentGroupManager->setGroupFlushFront(groupId, flushFront, false);
            _segmentGroupManager->setGroupWriteFront(groupId, flushFront, false);
        }
        // remove the segment and group after processing (except when break after GC)
        if (_centralizedReservedPool[flushingPoolIndex].segmentsInPool.count(mainSegmentId) == 0 || _centralizedReservedPool[flushingPoolIndex].segmentsInPool.at(mainSegmentId).second.empty()) {
            _centralizedReservedPool[flushingPoolIndex].segmentsInPool.erase(mainSegmentId);
            _centralizedReservedPool[flushingPoolIndex].groupsInPool.erase(groupId);
        }
    }

    if (gcCrashConsistency) {
        offset_t flushFront = _segmentGroupManager->getGroupFlushFront(groupId, false);
        // remove redundant information
        //size_t noneed = 0;
        for (size_t i = 0; i < keys.size(); i++) {
            if (oldLocations->at((unsigned char*) keys.at(i)) >= flushFront) {
                values.at(i).value.clear();
                //noneed ++;
            }
        }
        //printf("%lu keys no need to save %lu\n", keys.size(), noneed);
        
        // write the consistency log
        std::map<group_id_t, std::pair<offset_t, std::vector<segment_id_t> > > groups;
        for (auto g : modifiedGroups) {
            groups[g] = 
                    std::pair<offset_t, std::vector<segment_id_t> > (
                            _segmentGroupManager->getGroupFlushFront(g, /* needsLock = */ false),
                            _segmentGroupManager->getGroupSegments(g, /* needsLock = */ false)
                    );
        }
        _logManager->setBatchGCKeyValue(keys, values, groups);
        // write the data (last segment, other previous segments)
        if (Segment::getId(segment) != INVALID_SEGMENT) {
            tmpSegments.push_back(segment);
        }
        for (auto c : tmpSegments) {
            segment = c;
            inSegmentOffset = 0;
            FLUSH_SEGMENT_BUFFER(0);
        }
    }

    // wait until all data are flushed
    while (waitIO > 0);

    // write update consistency log
    if (isUpdate && ConfigManager::getInstance().enableCrashConsistency()) {
        std::map<group_id_t, std::pair<offset_t, std::vector<segment_id_t> > > groups;
        for (auto g : modifiedGroups) {
            groups[g] = 
                    std::pair<offset_t, std::vector<segment_id_t> > (
                            _segmentGroupManager->getGroupFlushFront(g, /* needsLock = */ false),
                            _segmentGroupManager->getGroupSegments(g, /* needsLock = */ false)
                    );
        }
        _logManager->setBatchUpdateKeyValue(keys, values, groups);
    }

    // update LSM-tree
    gettimeofday(&keyWriteStartTime, 0);
    if (!keys.empty() && _keyManager->writeKeyBatch(keys, values) == false) {
        debug_error("Failed to set %lu keys\n", keys.size());
    }

    // update persist log metadata
    logMetaPersist(modifiedSegments, modifiedGroups);

    // remove update consistency log
    if (isUpdate) {
        _logManager->ackBatchUpdateKeyValue();
    } else {
        _logManager->ackBatchGCKeyValue();
    }

    StatsRecorder::getInstance()->timeProcess(isUpdate? StatsType::UPDATE_KEY_WRITE_LSM : StatsType::UPDATE_KEY_WRITE_LSM_GC, keyWriteStartTime, /* diff = */ 0, /* count = */ keys.size());
    StatsRecorder::getInstance()->timeProcess(StatsType::GROUP_IN_POOL_FLUSH, keyWriteStartTime, /* diff = */ 0, /* count = */ keys.size());

    // free all tmp buffers
    for (auto c : tmpSegments) {
        Segment::free(c);
    }

    _centralizedReservedPool[flushingPoolIndex].keysInPool.clear();

    // should be empty already when all data are flushed (?)
    assert(_centralizedReservedPool[flushingPoolIndex].groupsInPool.empty());
    assert(_centralizedReservedPool[flushingPoolIndex].segmentsInPool.empty());

    // not necessary to clean, but reset
    Segment::resetFronts(_centralizedReservedPool[flushingPoolIndex].pool);

#undef FLUSH_SEGMENT_BUFFER
#undef RESET_SEGMENT_BUFFER
}

void ValueManager::flushCentralizedReservedPoolVLog (int poolIndex) {
    // directly write the pool out
    _centralizedReservedPool[poolIndex].lock.lock();

    Segment &pool = _centralizedReservedPool[poolIndex].pool;
    len_t writeLength = INVALID_LEN;
    offset_t logOffset = INVALID_OFFSET;
    std::tie(logOffset, writeLength) = flushSegmentToWriteFront(pool, false);
    // nonthing to flush
    if (writeLength == 0) {
        _centralizedReservedPool[poolIndex].lock.unlock();
        return;
    }
    ConfigManager &cm = ConfigManager::getInstance();
    // update metadata in LSM
    std::vector<char*> keys;
    std::vector<ValueLocation> values;
    len_t vs = INVALID_LEN;
    ValueLocation valueLoc;
    valueLoc.segmentId = _isSlave? cm.getNumSegment() : 0;
    len_t capacity = _isSlave? cm.getColdStorageCapacity() : cm.getSystemEffectiveCapacity();
    for (auto c : _centralizedReservedPool[poolIndex].segmentsInPool) {
        for (auto kv : c.second.second) {
            off_len_t offLen (kv + KEY_SIZE, sizeof(len_t));
            Segment::readData(pool, &vs, offLen);
            keys.push_back((char*)Segment::getData(pool) + kv);
            valueLoc.offset = (logOffset + kv) % capacity;
            valueLoc.length = vs;
            values.push_back(valueLoc);
            //printf("Flush update to key %x%x at offset %lu value %x of length %lu\n", kv.first[0], kv.first[KEY_SIZE-1], valueLoc.offset, kv.first[KEY_SIZE + sizeof(len_t)], vs);
        }
    }
    STAT_TIME_PROCESS(_keyManager->writeKeyBatch(keys, values), StatsType::UPDATE_KEY_WRITE_LSM);
    if (ConfigManager::getInstance().persistLogMeta()) {
        _keyManager->writeMeta(SegmentGroupManager::LogTailString, strlen(SegmentGroupManager::LogTailString), to_string(logOffset + writeLength));
        _keyManager->writeMeta(SegmentGroupManager::LogValidByteString, strlen(SegmentGroupManager::LogValidByteString), to_string(_slave.validBytes));
        _keyManager->writeMeta(SegmentGroupManager::LogWrittenByteString, strlen(SegmentGroupManager::LogWrittenByteString), to_string(_slave.writtenBytes));
    }
    // clean up the pool
    _centralizedReservedPool[poolIndex].groupsInPool.clear();
    _centralizedReservedPool[poolIndex].segmentsInPool.clear();
    _centralizedReservedPool[poolIndex].keysInPool.clear();
    Segment::resetFronts(_centralizedReservedPool[poolIndex].pool);

    _centralizedReservedPool[poolIndex].lock.unlock();
}

std::pair<offset_t, len_t> ValueManager::flushSegmentToWriteFront(Segment &segment, bool isGC) {
    assert(_isSlave || ConfigManager::getInstance().enabledVLogMode());

    segment_len_t flushFront = Segment::getFlushFront(segment);
    len_t writeLength = Segment::getWriteFront(segment) - flushFront;

    if (writeLength == 0)
        return std::pair<offset_t, len_t> (INVALID_OFFSET, writeLength);

    offset_t logOffset = _segmentGroupManager->getAndIncrementVLogWriteOffset(writeLength, isGC);

    if (logOffset == INVALID_OFFSET) {
        assert(isGC == false);
        //printSlaveStats();
        _gcManager->gcVLog();
        logOffset = _segmentGroupManager->getAndIncrementVLogWriteOffset(writeLength);
    }

    assert(logOffset != INVALID_OFFSET);
    len_t ret = 0;
    if (ConfigManager::getInstance().segmentAsFile() && _isSlave) {
        ret = _deviceManager->writeDisk(ConfigManager::getInstance().segmentAsSeparateFile()? ConfigManager::getInstance().getNumSegment() : 0, Segment::getData(segment) + flushFront, logOffset, writeLength);
    } else {
        ret = _deviceManager->writeDisk(/* diskId = */ 0, Segment::getData(segment) + flushFront, logOffset, writeLength);
    }

    if (ret != writeLength) {
        debug_error("Failed to write updates at offset %lu of length %lu\n", logOffset, writeLength);
        assert(0);
    }

    return std::pair<offset_t, len_t> (logOffset, writeLength);
}

int ValueManager::getNextPoolIndex(int current) {
    return (current + 1) % ConfigManager::getInstance().getNumPipelinedBuffer();
}

void ValueManager::decrementPoolIndex(int &current) {
    int numPipelinedBuffers = ConfigManager::getInstance().getNumPipelinedBuffer();
    current = (current + numPipelinedBuffers - 1) % numPipelinedBuffers;
}

void ValueManager::flushCentralizedReservedPoolBg(StatsType stats) {
    int nextPoolIndex = getNextPoolIndex(_centralizedReservedPoolIndex.inUsed);
    // wait until next available buffer is available after flush
    // (assume multiple core is available)
    // the buffer into queue for flush
    while (nextPoolIndex == _centralizedReservedPoolIndex.flushNext) {
        pthread_cond_wait(&_centralizedReservedPoolIndex.flushedBuffer, &_centralizedReservedPoolIndex.queueLock);
    }
    //printf("Put pool %d into queue\n", _centralizedReservedPoolIndex.inUsed);
    _centralizedReservedPoolIndex.queue.push(std::pair<int, StatsType>(_centralizedReservedPoolIndex.inUsed, stats));
    pthread_mutex_unlock(&_centralizedReservedPoolIndex.queueLock);
    // increment the index of pool to use
    _centralizedReservedPoolIndex.inUsed = nextPoolIndex;
    //printf("Going to use pool %d\n", _centralizedReservedPoolIndex.inUsed);
    // signal the worker to wake and process buffers in queue
    pthread_cond_signal(&_needBgFlush);
}

void* ValueManager::flushCentralizedReservedPoolBgWorker(void *arg) {
    ValueManager *instance = (ValueManager *) arg;
    // loop until valueManager is destoryed
    //fprintf(stderr, "Bg flush thread starts now, hello\n");
    while (instance->_started) {
        // wait for signal after data is put into queue
        pthread_cond_wait(&instance->_needBgFlush, &instance->_centralizedReservedPoolIndex.queueLock);
        // lock before queue checking
        while (!instance->_centralizedReservedPoolIndex.queue.empty()) {
            int poolIndex;
            StatsType stats;
            tie(poolIndex, stats) = instance->_centralizedReservedPoolIndex.queue.front();
            instance->_centralizedReservedPoolIndex.queue.pop();
            // unlock to allow producer to push items in while processing the current one
            pthread_mutex_unlock(&instance->_centralizedReservedPoolIndex.queueLock);
            // do the flushing as usual
            //printf("Pull and flush pool %d from queue\n", poolIndex);
            STAT_TIME_PROCESS(instance->flushCentralizedReservedPool(/* *reportGroupId = */ 0, /* isUpdate = */ true, poolIndex), stats);
            instance->_centralizedReservedPoolIndex.flushNext = instance->getNextPoolIndex(instance->_centralizedReservedPoolIndex.flushNext);
            //printf("Complete processing pool %d next %d \n", poolIndex, instance->_centralizedReservedPoolIndex.flushNext);
            // lock before queue checking
            pthread_cond_signal(&instance->_centralizedReservedPoolIndex.flushedBuffer);
            pthread_mutex_lock(&instance->_centralizedReservedPoolIndex.queueLock);
        };
        // leave the mutex to pthread_cond_wait to unlock
    }
    //fprintf(stderr, "Bg flush thread exits now, bye\n");

    return (void *) 0;
}

offset_t ValueManager::getLastSegmentFront(offset_t flushFront) {
    segment_len_t mainSegmentSize = ConfigManager::getInstance().getMainSegmentSize();
    segment_len_t logSegmentSize = ConfigManager::getInstance().getLogSegmentSize();
    if (flushFront <= mainSegmentSize) {
        return flushFront % mainSegmentSize;
    } else {
        offset_t front = (flushFront - mainSegmentSize) % logSegmentSize;
        if (front == 0) {
            return logSegmentSize;
        } else {
            return front;
        }
    }
    return 0;
}

void ValueManager::logMetaPersist(std::set<segment_id_t> &modifiedSegments, std::set<group_id_t> &modifiedGroups) {
    // segments
    for (segment_id_t cid : modifiedSegments) {
        _segmentGroupManager->writeSegmentMeta(cid);
    }
    // group metadata
    for (group_id_t gid : modifiedGroups) {
        _segmentGroupManager->writeGroupMeta(gid);
    }
}

void ValueManager::restoreFromUpdateLog() {
    std::vector<std::string> keys;
    std::vector<ValueLocation> values;
    std::map<group_id_t, std::pair<offset_t, std::vector<segment_id_t> > > groups;
    std::map<segment_id_t, offset_t> segments;
    // update log
    if (_logManager->readBatchUpdateKeyValue(keys, values, groups)) {
        // update kv locations
        _keyManager->writeKeyBatch(keys, values);
        // update group metadata
        for (auto g : groups) {
            //printf("Log group %lu front b4 %lu aft %lu\n", g.first, _segmentGroupManager->getGroupWriteFront(g.first, /* needsLock = */ false), g.second.first);
            // in-memory
            _segmentGroupManager->setGroupWriteFront(g.first, g.second.first, /* needsLock = */ false);
            _segmentGroupManager->setGroupFlushFront(g.first, g.second.first, /* needsLock = */ false);
            _segmentGroupManager->setGroupSegments(g.first, g.second.second);
            // LSM-tree
            _segmentGroupManager->writeGroupMeta(g.first);
        }
        // update segment metadata
        for (size_t i = 0; i < keys.size(); i++) {
            segment_id_t cid = values.at(i).segmentId;
            if (cid == LSM_SEGMENT) {
                assert(0);
                continue;
            }
            offset_t writeFront = values.at(i).offset + values.at(i).length + KEY_SIZE;
            if (segments.count(cid) == 0 || segments.at(cid) < writeFront) {
                segments[cid] = writeFront;
            }
        }
        for (auto c : segments) {
            //printf("Log segment %lu front %lu\n", c.first, c.second);
            _segmentGroupManager->setSegmentFlushFront(c.first, c.second);
            _segmentGroupManager->writeSegmentMeta(c.first);
        }
        // remove the log file
        _logManager->ackBatchUpdateKeyValue();
    }
}

void ValueManager::restoreFromGCLog() {
    std::vector<std::string> keys;
    std::vector<ValueLocation> values;
    std::map<group_id_t, std::pair<offset_t, std::vector<segment_id_t> > > groups;
    std::map<segment_id_t, Segment> segments;

    if (_logManager->readBatchGCKeyValue(keys, values, groups)) {
        ConfigManager &cm = ConfigManager::getInstance();
        segment_len_t numMainSegments = cm.getNumMainSegment();
        len_t mainSegmentSize = cm.getMainSegmentSize();
        len_t logSegmentSize = cm.getLogSegmentSize();

        std::map<segment_id_t, Segment> segmentBuf;
        // make sure all values are there, and create segment buffers
        for (size_t i = 0; i < keys.size(); i++) {
            len_t valueSize = values.at(i).length;
            // read the yet overwritten value from segment
            if (values.at(i).value.empty()) {
                ValueLocation oldValueLoc = _keyManager->getKey(keys.at(i).c_str(), KEY_SIZE);
                char *value = 0;
                len_t valueSize = 0;
                getValueFromDisk(keys.at(i).c_str(), oldValueLoc, value, valueSize);
                assert(valueSize + sizeof(len_t) == values.at(i).length);
                values.at(i).value = std::string(value, valueSize);
                delete value;
            } else {
                valueSize = values.at(i).length;
                if (values.at(i).length > 0) {
                    valueSize -= sizeof(len_t);
                }
            }
            segment_id_t cid = values.at(i).segmentId;
            if (segmentBuf.count(cid) == 0) {
                Segment::init(segmentBuf[cid], cid, cid >= numMainSegments? logSegmentSize : mainSegmentSize, /* needsSetZero = */ false);
            }
            segment_off_len_t offLen = {values.at(i).offset, KEY_SIZE};
            Segment::overwriteData(segmentBuf[cid], keys.at(i).c_str(), offLen);
            offLen.first += KEY_SIZE;
            offLen.second = sizeof(len_t);
            Segment::overwriteData(segmentBuf[cid], &valueSize, offLen);
            offLen.first += sizeof(len_t);
            offLen.second = valueSize;
            Segment::overwriteData(segmentBuf[cid], values.at(i).value.c_str(), offLen);
            if (offLen.first + valueSize > Segment::getWriteFront(segmentBuf.at(cid))) {
                Segment::setWriteFront(segmentBuf.at(cid), offLen.first + valueSize);
                Segment::setFlushFront(segmentBuf.at(cid), offLen.first + valueSize);
            }
        }
        // write the segments
        std::atomic<int> waitIO;
        waitIO = 0;
        offset_t writeOffset = 0;
        for (auto segment : segments) {
            waitIO += 1;
            _iothreads.schedule(
                    std::bind(
                        &DeviceManager::writePartialSegmentMt,
                        _deviceManager,
                        Segment::getId(segment.second),
                        Segment::getFlushFront(segment.second),
                        Segment::getWriteFront(segment.second) - Segment::getFlushFront(segment.second),
                        Segment::getData(segment.second) + Segment::getFlushFront(segment.second),
                        boost::ref(writeOffset),
                        boost::ref(waitIO)
                    )
            );
        }
        while (waitIO > 0);
        // update the segment metadata
        for (auto segment : segments) {
            _segmentGroupManager->setSegmentFlushFront(segment.first, Segment::getWriteFront(segment.second));
            _segmentGroupManager->writeSegmentMeta(segment.first);
        }
        // update the group metadata 
        for (auto g : groups) {
            // in-memory
            _segmentGroupManager->setGroupWriteFront(g.first, g.second.first, /* needsLock = */ false);
            _segmentGroupManager->setGroupFlushFront(g.first, g.second.first, /* needsLock = */ false);
            _segmentGroupManager->setGroupSegments(g.first, g.second.second);
            // LSM-tree
            _segmentGroupManager->writeGroupMeta(g.first);
        }
        // remove gc log file
        _logManager->ackBatchGCKeyValue();
    }
}

bool ValueManager::forceSync() {
    std::lock_guard<std::mutex> gcLock (_GCLock);
    if (ConfigManager::getInstance().enabledVLogMode() || _isSlave) {
        STAT_TIME_PROCESS(flushCentralizedReservedPoolVLog(), POOL_FLUSH);
    } else {
        STAT_TIME_PROCESS(flushCentralizedReservedPool(), POOL_FLUSH);
    }
    return true;
}

void ValueManager::printSlaveStats(FILE *out) {
    if (_isSlave) {
        fprintf(out,
                "(This) Slave capacity: %lu; In-use: %lu; Valid: %lu\n"
                , ConfigManager::getInstance().getColdStorageCapacity()
                , _slave.writtenBytes
                , _slave.validBytes
               );
        _gcManager->printStats(out);
    } else if (ConfigManager::getInstance().useSlave() && _slaveValueManager && _slave.gcm) {
        fprintf(out,
                "Slave capacity: %lu; In-use: %lu; Valid: %lu\n"
                , ConfigManager::getInstance().getColdStorageCapacity()
                , _slaveValueManager->_slave.writtenBytes
                , _slaveValueManager->_slave.validBytes
               );
        _slave.gcm->printStats(out);
    }
}

#undef RECORD_SIZE
