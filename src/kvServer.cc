#include <stdlib.h>
#include "../util/debug.hh"
#include "../util/timer.hh"
#include "kvServer.hh"
#include "leveldbKeyManager.hh"
#include "statsRecorder.hh"

KvServer::KvServer() : KvServer(0) {
}

KvServer::KvServer(DeviceManager *deviceManager) {
    // devices
    if (deviceManager) {
        _deviceManager = deviceManager;
    } else {
        _deviceManager = new DeviceManager();
    }
    _freeDeviceManager = (deviceManager == 0);
    // metadata log
    _logManager = new LogManager(deviceManager);
    // keys and values
    _keyManager = new LevelDBKeyManager(ConfigManager::getInstance().getLSMTreeDir().c_str());
    // segments and groups
    _segmentGroupManager = new SegmentGroupManager(/* isSlave = */ false, _keyManager);
    // values
    _valueManager = new ValueManager(_deviceManager, _segmentGroupManager, _keyManager, _logManager);
    // gc
    _gcManager = new GCManager(_keyManager, _valueManager, _deviceManager, _segmentGroupManager);
    
    _valueManager->setGCManager(_gcManager);

    _scanthreads.size_controller().resize(ConfigManager::getInstance().getNumRangeScanThread());
}

KvServer::~KvServer() {
    delete _valueManager;
    delete _keyManager;
    delete _gcManager;
    delete _segmentGroupManager;
    delete _logManager;
    if (_freeDeviceManager)
        delete _deviceManager;
}

bool KvServer::checkKeySize(len_t &keySize) {
    if (keySize != KEY_SIZE)
        debug_error("Variable key size is not supported, key size must be %d.\n", KEY_SIZE);
    return (keySize == KEY_SIZE);
}

bool KvServer::putValue(char *key, len_t keySize, char *value, len_t valueSize) {
    bool ret = false;
    ValueLocation curValueLoc, oldValueLoc;
    oldValueLoc.value.clear();
    // only support fixed key size
    if (checkKeySize(keySize) == false)
        return ret;
    if (valueSize >= ConfigManager::getInstance().getMainSegmentSize()) {
        debug_error("Value size larger than segment size is not supported (at most %lu).\n", ConfigManager::getInstance().getMainSegmentSize());
        return ret;
    }

    int retry = 0;
    // identify values writing directly to LSM-tree (1) smaller than threshold, or (2) key-value separation is disabled
    bool toLSM = valueSize < ConfigManager::getInstance().getMinValueSizeToLog() || ConfigManager::getInstance().disableKvSeparation();

retry_update:
    struct timeval keyLookupStartTime;
    gettimeofday(&keyLookupStartTime, 0);
    if (ConfigManager::getInstance().enabledVLogMode()) {
        // vlog virtually put all writes to one group
        oldValueLoc.segmentId = 0;
    } else if (toLSM) {
        // write to LSM-tree directly
        oldValueLoc.segmentId = LSM_SEGMENT;
    } else {
        // find the deterministic location
        oldValueLoc.segmentId = HashFunc::hash(key, KEY_SIZE) % ConfigManager::getInstance().getNumMainSegment();
        // always allocate the group if not exists
        group_id_t groupId = INVALID_GROUP;
        _segmentGroupManager->getNewMainSegment(groupId, oldValueLoc.segmentId, /* needsLock */ false);
    }
    bool inLSM = oldValueLoc.segmentId == LSM_SEGMENT;
    StatsRecorder::getInstance()->timeProcess(StatsType::UPDATE_KEY_LOOKUP, keyLookupStartTime);
    // update the value of the key, get the new location of value
    STAT_TIME_PROCESS(curValueLoc = _valueManager->putValue(key, keySize, value, valueSize, oldValueLoc), StatsType::UPDATE_VALUE);
    debug_info("Update key %x%x to segment id=%lu,ofs=%lu,len=%lu\n", key[0], key[KEY_SIZE-1], curValueLoc.segmentId, curValueLoc.offset, curValueLoc.length);
    // retry for UPDATE if failed (due to GC)
    if (!inLSM && curValueLoc.segmentId == INVALID_SEGMENT) {
        // best effort retry
        if (retry++ <= ConfigManager::getInstance().getRetryMax()) {
            debug_warn("Retry for update %d time(s)\n", retry);
            goto retry_update;
        }
        // report set failure
        debug_error("Failed to write value for key %x%x!\n", key[0], key[KEY_SIZE-1]);
        assert(0);
        return ret;
    } else {
        ret = (curValueLoc.length == valueSize + (curValueLoc.segmentId == LSM_SEGMENT? 0 : sizeof(len_t)));
    }
    return ret;
}

void KvServer::getValueMt(char *key, len_t keySize, char *&value, len_t &valueSize, ValueLocation valueLoc, uint8_t &ret, std::atomic<size_t> &keysInProcess) {

    // get value using the location
    ret = (_valueManager->getValueFromBuffer(key, value, valueSize));

    // search on disk
    if (!ret && !ConfigManager::getInstance().disableKvSeparation() && valueLoc.segmentId != INVALID_SEGMENT) {
        ret = _valueManager->getValueFromDisk(key, valueLoc, value, valueSize);
    }

    keysInProcess--;
}

bool KvServer::getValue(char *key, len_t keySize, char *&value, len_t &valueSize, bool timed) {
    bool ret = false;

    if (checkKeySize(keySize) == false)
        return ret;

    struct timeval startTime;
    gettimeofday(&startTime, 0);

    // get value using the location
    ret = (_valueManager->getValueFromBuffer(key, value, valueSize));

    if (ret) {
        StatsRecorder::getInstance()->timeProcess(StatsType::GET_VALUE, startTime);
        return ret;
    }

    ValueLocation readValueLoc;
    // get the value's location
    STAT_TIME_PROCESS(readValueLoc = _keyManager->getKey(key), StatsType::GET_KEY_LOOKUP);

    // found for selective key-value separation or disabled key-value separation
    bool disableKvSep = ConfigManager::getInstance().disableKvSeparation();
    if ((readValueLoc.segmentId == LSM_SEGMENT || disableKvSep /* no segment id */) && readValueLoc.length != INVALID_LEN) {
        // key-value pairs found entirely in LSM
        value = new char [readValueLoc.length];
        valueSize = readValueLoc.length;
        readValueLoc.value.copy(value, valueSize);
        if (timed) StatsRecorder::getInstance()->timeProcess(StatsType::GET_VALUE, startTime);
        return true;
    }

    // not found
    if (readValueLoc.segmentId == INVALID_SEGMENT) return false;

    ret = _valueManager->getValueFromDisk(key, readValueLoc, value, valueSize);
    if (timed) StatsRecorder::getInstance()->timeProcess(StatsType::GET_VALUE, startTime);

    return ret;
}

void KvServer::getRangeValues(char *startingKey, uint32_t numKeys, std::vector<char*> &keys, std::vector<char*> &values, std::vector<len_t> &valueSize) {
    struct timeval startTime;
    gettimeofday(&startTime, 0);

    std::vector<uint8_t> rets;
    std::vector<ValueLocation> locs;
    keys.clear();
    values.resize(numKeys);
    valueSize.resize(numKeys);
    rets.resize(numKeys);
    locs.resize(numKeys);

    // Todo: range scan on LSM-tree to get the keys
    //_keyManager->getKeys(startingKey, numKeys, keys, locs);

    // keep track of the number of keys to process
    std::atomic<size_t> keysInProcess;
    keysInProcess = 0;

    bool disableKvSep = ConfigManager::getInstance().disableKvSeparation();
    KeyManager::KeyIterator *kit = _keyManager->getKeyIterator(startingKey);
    char *key = 0;

    for (uint32_t i = 0; i < numKeys && kit->isValid(); i++, kit->next()) {
        // get the key
        key = new char [KEY_SIZE];
        memcpy(key, kit->key().c_str(), KEY_SIZE);
        keys.push_back(key);

        // lookup the location
        locs.at(i).deserialize(kit->value()); 
        if ((locs.at(i).segmentId == LSM_SEGMENT || disableKvSep /* no segment id */) && locs.at(i).length != INVALID_LEN) {
            // key-value pairs found entirely in LSM
            valueSize.at(i) = locs.at(i).length;
            values.at(i) = new char [valueSize.at(i)];
            locs.at(i).value.copy(values.at(i), valueSize.at(i));
        }

        keysInProcess += 1;

        if (ConfigManager::getInstance().enabledScanReadAhead() && locs.at(i).segmentId != LSM_SEGMENT && locs.at(i).segmentId != INVALID_SEGMENT) {
            _deviceManager->readAhead(locs.at(i).segmentId, locs.at(i).offset, locs.at(i).length + KEY_SIZE + sizeof(len_t));;
        }

        // search into buffer and disk in parallel 
        _scanthreads.schedule(
                std::bind(
                    &KvServer::getValueMt,
                    this,
                    keys.at(i),
                    KEY_SIZE,
                    boost::ref(values.at(i)),
                    boost::ref(valueSize.at(i)),
                    locs.at(i),
                    boost::ref(rets.at(i)),
                    boost::ref(keysInProcess)
                )
        );
    }

    kit->release();
    delete kit;

    while (keysInProcess > 0);

    StatsRecorder::getInstance()->timeProcess(StatsType::GET_VALUE, startTime);
}

bool KvServer::delValue(char *key, len_t keySize) {
    int retry = 0;
    ValueLocation valueLoc, retValueLoc;
    if (checkKeySize(keySize) == false)
        return false;
    // get the value's location
    valueLoc = _keyManager->getKey(key);
    if (valueLoc.segmentId == INVALID_SEGMENT) {
        debug_warn("Value for key %x%x not found.\n", key[0], key[KEY_SIZE-1]);
        return false;
    }
    while (retValueLoc.segmentId == INVALID_SEGMENT) {
        // best effort retry
        if (retry++ > ConfigManager::getInstance().getRetryMax())
            break;
        retValueLoc = _valueManager->putValue(key, keySize, 0, INVALID_LEN, valueLoc, 1);
    }
    // always delete the key first ..
    _keyManager->deleteKey(key);
    return (retValueLoc.segmentId != INVALID_SEGMENT);
}

bool KvServer::flushBuffer() {
    return _valueManager->forceSync();
}

size_t KvServer::gc(bool all) {
    return (all? _gcManager->gcAll() : _gcManager->gcGreedy());
}

void KvServer::printStorageUsage(FILE *out) {
    _segmentGroupManager->printUsage(out);
}

void KvServer::printGroups(FILE *out) {
    _segmentGroupManager->printGroups(out);
}

void KvServer::printBufferUsage(FILE *out) {
    //_valueManager->printUsage(out);
}

void KvServer::printKeyCacheUsage(FILE *out) {
    _keyManager->printCacheUsage(out);
}

void KvServer::printKeyStats(FILE *out) {
    _keyManager->printStats(out);
}

void KvServer::printValueSlaveStats(FILE *out) {
    _valueManager->printSlaveStats(out);
}

void KvServer::printGCStats(FILE *out) {
    _gcManager->printStats(out);
}
