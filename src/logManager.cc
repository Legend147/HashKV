#include <atomic>
#include "logManager.hh"
#include "segmentGroupManager.hh"

const char *LogManager::LOG_MAGIC = "HA5H_KV_3ND_0F_70G";

LogManager::LogManager(DeviceManager *deviceManager) {
    _deviceManager = deviceManager;

    _logAcked.update = true;
    _logAcked.gc = true;
    _enabled = ConfigManager::getInstance().enableCrashConsistency();

    if (_enabled) {
        //segment_len_t segmentSize = ConfigManager::getInstance().getMainSegmentSize();
        // allocate buffer
        size_t logBufSize =  128 * 1024 * 1024;
        bool ret = Segment::init(_buffer.dataSegment, 0, logBufSize);
        if (!ret) {
            debug_error("Failed to allocate log buffer of size %lu\n", logBufSize);
            assert(0);
            exit(-1);
        }   
    }

}

LogManager::~LogManager() {
    // free buffer
    Segment::free(_buffer.dataSegment);
}

bool LogManager::setBatchUpdateKeyValue(std::vector<char *> &keys, std::vector<ValueLocation> &values, std::map<group_id_t, std::pair<offset_t, std::vector<segment_id_t> > > &groups) {
    if (!_enabled) return true;

    assert(keys.size() == values.size());
    if (keys.empty())
        return false;

    bool ret = setBatchKeyValue(keys, values, groups, /* isUpdate = */ true);

    if (!ret) return false;

    _logAcked.update = false;
    _deviceManager->writeUpdateLog(Segment::getData(_buffer.dataSegment), Segment::getWriteFront(_buffer.dataSegment));
    //printf("Update %lu keys to update log length = %lu\n", keys.size(), Segment::getWriteFront(_buffer.dataSegment));
    Segment::resetFronts(_buffer.dataSegment);

    return true;
}

bool LogManager::setBatchGCKeyValue(std::vector<char *> &keys, std::vector<ValueLocation> &values, std::map<group_id_t, std::pair<offset_t, std::vector<segment_id_t> > > &groups) {
    if (!_enabled) return true;

    assert(keys.size() == values.size());
    if (keys.empty())
        return false;

    bool ret = setBatchKeyValue(keys, values, groups, /* isUpdate = */ false);

    if (!ret) return false;

    _logAcked.update = false;
    _deviceManager->writeGCLog(Segment::getData(_buffer.dataSegment), Segment::getWriteFront(_buffer.dataSegment));
    //printf("Update %lu keys to GC log length = %lu\n", keys.size(), Segment::getWriteFront(_buffer.dataSegment));
    Segment::resetFronts(_buffer.dataSegment);

    return true;
}

bool LogManager::setBatchKeyValue(std::vector<char *> &keys, std::vector<ValueLocation> &values, std::map<group_id_t, std::pair<offset_t, std::vector<segment_id_t> > > &groups, bool isUpdate) {
    std::lock_guard<std::mutex> lk (_buffer.lock);
    assert(keys.size() == values.size());
    if (keys.empty())
        return false;

    // groups
    size_t groupTotal = groups.size();
    Segment::appendData(_buffer.dataSegment, &groupTotal, sizeof(size_t)); 
    for (auto g : groups) {
        Segment::appendData(_buffer.dataSegment, &g.first, sizeof(group_id_t));
        std::string list = SegmentGroupManager::generateGroupValue(g.second.first, g.second.second);
        size_t listLen = list.length();
        Segment::appendData(_buffer.dataSegment, &listLen, sizeof(listLen));
        Segment::appendData(_buffer.dataSegment, list.c_str(), listLen);
    }

    // kv pairs
    size_t keyTotal = keys.size();
    Segment::appendData(_buffer.dataSegment, &keyTotal, sizeof(size_t));

    len_t recordSize = KEY_SIZE + sizeof(segment_id_t) + sizeof(offset_t) + sizeof(len_t);
    for (size_t i = 0; i < keyTotal; i++) {
        len_t valueLength = values.at(i).value.length();
        if (!isUpdate && valueLength > 0) {
            recordSize = KEY_SIZE + sizeof(segment_id_t) + sizeof(offset_t) + sizeof(len_t) + valueLength;
        }
        if (Segment::canFit(_buffer.dataSegment, recordSize) == false) {
            printf("error buf = %p %lu\n", &_buffer.dataSegment, Segment::getSize(_buffer.dataSegment));
            debug_error("Log buffer is too small (size = %lu, num. of keys = %lu, next size = %lu)!\n", Segment::getSize(_buffer.dataSegment), keys.size(), recordSize);
            assert(0);
            exit(-1);
        }
        std::string loc = values.at(i).serialize();
        size_t locLength = loc.length();
        // key
        Segment::appendData(_buffer.dataSegment, keys.at(i), KEY_SIZE);
        // value location length
        Segment::appendData(_buffer.dataSegment, &locLength, sizeof(locLength));
        // value location
        Segment::appendData(_buffer.dataSegment, loc.c_str(), locLength);
        if (!isUpdate) {
            // value length
            Segment::appendData(_buffer.dataSegment, &valueLength, sizeof(len_t));
            // value (if exists)
            if (valueLength > 0) {
                Segment::appendData(_buffer.dataSegment, values.at(i).value.c_str(), valueLength);
            }
        }
    }

    // end magic for consistency log
    Segment::appendData(_buffer.dataSegment, LOG_MAGIC, strlen(LOG_MAGIC));

    return true;
}

bool LogManager::readBatchUpdateKeyValue(std::vector<std::string> &keys, std::vector<ValueLocation> &values, std::map<group_id_t, std::pair<offset_t, std::vector<segment_id_t> > > &groups) {
    return readBatchKeyValue(keys, values, groups, /* isUpdate = */ true);
}

bool LogManager::readBatchGCKeyValue(std::vector<std::string> &keys, std::vector<ValueLocation> &values, std::map<group_id_t, std::pair<offset_t, std::vector<segment_id_t> > > &groups, bool removeIfCorrupted) {
    return readBatchKeyValue(keys, values, groups, /* isUpdate = */ false, removeIfCorrupted);
}

bool LogManager::readBatchKeyValue(std::vector<std::string> &keys, std::vector<ValueLocation> &values, std::map<group_id_t, std::pair<offset_t, std::vector<segment_id_t> > > &groups, bool isUpdate, bool removeIfCorrupted) {
    std::lock_guard<std::mutex> lk (_buffer.lock);

    len_t logSize = isUpdate? _deviceManager->getUpdateLogSize() : _deviceManager->getGCLogSize();

    // no log available
    if (logSize == 0) return false;

    // create a tmp buffer to read key values
    Segment::init(_buffer.readSegment, INVALID_SEGMENT, logSize, /* needsSetZero = */ false);

    unsigned char *bufData = Segment::getData(_buffer.readSegment);

    if (isUpdate) {
        _deviceManager->readUpdateLog(bufData, logSize);
    } else {
        _deviceManager->readGCLog(bufData, logSize);
    }

    // check the magic at the end of gc consistency log 
    bool completeLog = memcmp(bufData + logSize - strlen(LOG_MAGIC), LOG_MAGIC, strlen(LOG_MAGIC)) == 0;
    if (!completeLog) {
        if (removeIfCorrupted) {
            if (isUpdate) {
                ackBatchUpdateKeyValue();
             } else {
                ackBatchGCKeyValue();
             }
        }
        debug_error("Cannot recover from incomplete %s log\n", isUpdate? "update" : "gc");
        return false;
    }

    logSize -= strlen(LOG_MAGIC);

    // start from scratch
    groups.clear();
    keys.clear();
    values.clear();

    len_t scanSize = 0;
    segment_off_len_t offLen (scanSize, sizeof(size_t));

#define CHECK_REMAINS(_LENGTH_) do { \
        if (scanSize + _LENGTH_ > logSize) { \
            scanSize = logSize; \
            break; \
        } \
    } while (0)

    // read groups
    CHECK_REMAINS(sizeof(size_t));
    size_t groupTotal = 0;
    Segment::readData(_buffer.readSegment, &groupTotal, offLen);
    scanSize += sizeof(size_t);

    for (size_t i = 0; i < groupTotal; i++) {
        // group id
        CHECK_REMAINS(sizeof(group_id_t));
        offLen = {scanSize, sizeof(group_id_t)};
        group_id_t groupId = INVALID_GROUP;
        Segment::readData(_buffer.readSegment, &groupId, offLen);
        scanSize += sizeof(group_id_t);
        // segment list length
        CHECK_REMAINS(sizeof(size_t));
        size_t listLength = 0;
        offLen = {scanSize, sizeof(size_t)};
        Segment::readData(_buffer.readSegment, &listLength, offLen);
        scanSize += sizeof(size_t);
        // segment list
        CHECK_REMAINS(listLength);
        std::string list ((char*) bufData + scanSize, listLength);
        std::pair<group_id_t, std::pair<offset_t, std::vector<segment_id_t> > > tg;
        SegmentGroupManager::readGroupMeta(list, tg.second.first, tg.second.second);
        scanSize += listLength;
        tg.first = groupId;
        groups.insert(tg);
    }

    // read the KV pairs
    size_t locLength = 0;
    len_t valueLength = 0;
    
    // total num of kv pairs
    size_t keyTotal = 0;
    if (scanSize < logSize) {
        offLen = {scanSize, sizeof(size_t)};
        Segment::readData(_buffer.readSegment, &keyTotal, offLen);
        scanSize += sizeof(size_t);
    } else {
        // no info on kv pair locations, no need to update group metadata
        assert(isUpdate);
        groups.clear();
        return false;
    }

    while (scanSize < logSize) {
        CHECK_REMAINS(KEY_SIZE);
        // key
        keys.push_back(std::string((char*) bufData + scanSize, KEY_SIZE));
        // value location length
        ValueLocation loc;
        segment_off_len_t offLen (scanSize + KEY_SIZE, sizeof(size_t));
        CHECK_REMAINS(KEY_SIZE + sizeof(size_t));
        Segment::readData(_buffer.readSegment, &locLength, offLen);
        assert(locLength > 0);
        // value location
        CHECK_REMAINS(KEY_SIZE + sizeof(size_t) + locLength);
        std::string valueLocStr ((char*) bufData + scanSize + KEY_SIZE + sizeof(size_t), locLength);
        loc.deserialize(valueLocStr);
        // value if exist
        if (!isUpdate) {
            // value length
            CHECK_REMAINS(KEY_SIZE + sizeof(size_t) + locLength + sizeof(len_t));
            offLen = {scanSize + KEY_SIZE + sizeof(size_t) + locLength, sizeof(len_t)};
            Segment::readData(_buffer.readSegment, &valueLength, offLen);
            if (valueLength > 0) {
                CHECK_REMAINS(KEY_SIZE + sizeof(size_t) + locLength + sizeof(len_t) + valueLength);
                loc.value = std::string((char*) bufData + scanSize + KEY_SIZE + sizeof(size_t) + locLength + sizeof(len_t), valueLength);
            }
            // update scan offset
            scanSize += KEY_SIZE + sizeof(size_t) + locLength + sizeof(len_t) + valueLength;
        } else {
            // update scan offset
            scanSize += KEY_SIZE + sizeof(size_t) + locLength;
        }
        // mark the scanned values 
        values.push_back(loc);
    }

    assert(keyTotal == values.size() && keyTotal == keys.size());

#undef CHECK_REMAINS

    // free the buffer
    Segment::free(_buffer.readSegment);

    return true;
}

bool LogManager::ackBatchUpdateKeyValue() {
    if (!_enabled) return true;

    bool ret = _deviceManager->removeUpdateLog();
    _logAcked.update = ret;

    return ret;
}

bool LogManager::ackBatchGCKeyValue() {
    if (!_enabled) return true;

    bool ret = _deviceManager->removeGCLog();
    _logAcked.gc = ret;

    return ret;
}

void LogManager::print(FILE *out) {
    fprintf(out,
            "Enabled         : %s\n"
            "Log usage:\n"
            " - Update log   : %s\n"
            " - GC log       : %s\n"
            , _enabled? "true" : "false"
            , _logAcked.update? "in-used" : "nil"
            , _logAcked.gc? "in-used" : "nil"
   );
}
