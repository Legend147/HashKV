#include <thread>
#include "configManager.hh"
#include "util/debug.hh"


void ConfigManager::setConfigPath (const char* path) {
    boost::property_tree::ini_parser::read_ini(path, _pt);
    assert (!_pt.empty());

    // basic
    _basic.mainSegmentSize = readULL("basic.mainSegmentSize");
    _basic.logSegmentSize  = readULL("basic.logSegmentSize");
    _basic.numMainSegment = readULL("basic.numMainSegment");
    _basic.numLogSegment = readULL("basic.numLogSegment");
    _basic.retryMax = readInt("basic.retryMax");
    _basic.segmentAsFile = readBool("basic.segmentAsFile");
    _basic.segmentAsSeparateFile = readBool("basic.separateSegmentFile");

    assert(_basic.mainSegmentSize > 0);
    assert(_basic.logSegmentSize > 0);
    assert(_basic.numMainSegment > 0);
    if (_basic.retryMax < 0) _basic.retryMax = 0;

    // buffer
    _buffer.updateKVBufferSize = readULL("buffer.updateKVBufferSize");
    _buffer.inPlaceUpdate= readBool("buffer.inPlaceUpdate");
    _buffer.numPipelinedBuffer = 1;
    if (_buffer.numPipelinedBuffer > MAX_CP_NUM) {
        _buffer.numPipelinedBuffer = MAX_CP_NUM;
    } else if (_buffer.numPipelinedBuffer < 1) {
        _buffer.numPipelinedBuffer = 1;
    }

    // hotness
    _hotness.levels = 2;
    //if (_hotness.levels <= 0) _hotness.levels = 1;
    _hotness.useSlave = readBool("hotness.coldVLog");
    if (!_hotness.useSlave) {
        _hotness.coldStorageSize = 0;
    } else {
        _hotness.coldStorageSize = readULL("hotness.coldStorageSize");
        if (_hotness.coldStorageSize == 0) {
             _hotness.useSlave = false;
        }
    }
    _hotness.coldStorageDevice = readString("hotness.coldStorageDevice");

    // key
    _key.lsmTreeDir = readString("key.lsmTreeDir");
    _key.locationCacheSize = 0;
    _key.dbType = readInt("key.useDB");
    _key.compress = readBool("key.useCompression");
    
    // logmeta
    _logmeta.persist = readBool("logmeta.persist");

    // gc
    _gc.greedyGCSize = readUInt("gc.greedyGCSize");
    if (_gc.greedyGCSize < 0) _gc.greedyGCSize = 1;
    _gc.mode = LOG_ONLY;
    _gc.numReadThread = readUInt("gc.numReadThread");
    if (_gc.numReadThread < 1) {
        _gc.numReadThread = 8;
    }

    // kv-separation
    _kvsep.minValueSizeToLog = readUInt("kvsep.minValueSizeToLog");
    if (_kvsep.minValueSizeToLog < 0) {
        _kvsep.minValueSizeToLog = 0;
    }
    _kvsep.disabled = readBool("kvsep.disabled");

    // vlog
    _vlog.enabled = readBool("vlog.enabled");
    _vlog.gcSize = readUInt("vlog.gcSize");
    if (_vlog.gcSize <= _buffer.updateKVBufferSize) {
        _vlog.gcSize = (_buffer.updateKVBufferSize == 0? 4096 : _buffer.updateKVBufferSize);
    }
    if (_vlog.enabled) {
        //_basic.logSegmentSize = 0;
        _basic.numLogSegment = 0;
        if (_kvsep.disabled) {
            debug_error("Invalid configuration with VLog enabled but KV-separation disabled (%d, %d)\n", _vlog.enabled, _kvsep.disabled);
            exit(-1);
        }
    } else {
        assert(_basic.numLogSegment > 0);
    }

    // consistency
    _consistency.crash = readBool("consistency.crashProtected");
    if (_consistency.crash && !_basic.segmentAsFile) {
        debug_error("Do not support block device crash consistency (%d, %d)\n", _consistency.crash, _basic.segmentAsFile);
        exit(-1);
    }
    if (_kvsep.disabled) {
        _consistency.crash = false;
    }

    // misc
    _misc.hashTableDefaultSize = readUInt("misc.hashTableDefaultSize");
    _misc.hashMethod = readInt("misc.hashMethod");
    _misc.numParallelFlush = readUInt("misc.numParallelFlush");
    _misc.numIoThread = readUInt("misc.numIoThread");
    _misc.numCPUThread = std::thread::hardware_concurrency();
    _misc.syncAfterWrite = readBool("misc.syncAfterWrite");
    _misc.numRangeScanThread = readUInt("misc.numRangeScanThread");
    _misc.scanReadAhead = readBool("misc.enableScanReadAhead");
    _misc.batchWriteThreshold = readInt("misc.writeBatchSize");
    _misc.useMmap = readBool("misc.enableMmap");
    _misc.maxOpenFiles = readInt("misc.maxOpenFiles");

    if (_misc.numParallelFlush == 0) _misc.numParallelFlush = 1;
    if (_misc.hashMethod <= 0) _misc.hashMethod = 0;
    if (_misc.hashTableDefaultSize == 0) _misc.hashTableDefaultSize = 128 * 1024;
    if (_misc.numIoThread <= 0) _misc.numIoThread = 1;
    if (_misc.numCPUThread <= 0) { _misc.numCPUThread = NUM_THREAD; }
    if (_misc.numRangeScanThread == 0) { _misc.numRangeScanThread = 1; }
    if (_misc.maxOpenFiles < -1) { _misc.maxOpenFiles = -1; }

    // debug
    _debug.level = (DebugLevel) readInt("debug.level");
    if (_debug.level < DebugLevel::NONE) { _debug.level = DebugLevel::NONE; }

    printConfig();
}

bool ConfigManager::readBool (const char* key) {
    return _pt.get<bool>(key);
}

int ConfigManager::readInt (const char* key) {
    return _pt.get<int>(key);
}

unsigned int ConfigManager::readUInt (const char* key) {
    return _pt.get<unsigned int>(key);
}

LL ConfigManager::readLL (const char* key) {
    return _pt.get<LL>(key);
}

ULL ConfigManager::readULL (const char* key) {
    return _pt.get<ULL>(key);
}

double ConfigManager::readFloat (const char* key) {
    return _pt.get<double>(key);
}

std::string ConfigManager::readString (const char* key) {
    return _pt.get<std::string>(key);
}

segment_len_t ConfigManager::getSegmentSize(bool isLog) const {
    assert (!_pt.empty());
    return (isLog)? _basic.logSegmentSize : _basic.mainSegmentSize;
}

segment_len_t ConfigManager::getMainSegmentSize() const {
    assert (!_pt.empty());
    return _basic.mainSegmentSize;
}

segment_len_t ConfigManager::getLogSegmentSize() const {
    assert (!_pt.empty());
    return _basic.logSegmentSize;
}

segment_len_t ConfigManager::getNumMainSegment() const {
    assert (!_pt.empty());
    return _basic.numMainSegment;
}

segment_len_t ConfigManager::getNumLogSegment() const {
    assert (!_pt.empty());
    return _basic.numLogSegment;
}

segment_len_t ConfigManager::getNumSegment() const {
    assert (!_pt.empty());
    return _basic.numMainSegment + _basic.numLogSegment;
}

len_t ConfigManager::getSystemEffectiveCapacity() const {
    assert (!_pt.empty());
    return (len_t) _basic.numMainSegment * _basic.mainSegmentSize;
}

int ConfigManager::getRetryMax() const {
    assert (!_pt.empty());
    return _basic.retryMax;
}

bool ConfigManager::segmentAsFile() const {
    assert (!_pt.empty());
    return _basic.segmentAsFile;
}

bool ConfigManager::segmentAsSeparateFile() const {
    assert (!_pt.empty());
    return _basic.segmentAsSeparateFile;
}

bool ConfigManager::isUpdateKVBufferEnabled() const {
    assert (!_pt.empty());
    return (_buffer.updateKVBufferSize > 0);
}

segment_len_t ConfigManager::getUpdateKVBufferSize() const {
    assert (!_pt.empty());
    return _buffer.updateKVBufferSize;
}

bool ConfigManager::isInPlaceUpdate() const {
    assert (!_pt.empty());
    return _buffer.inPlaceUpdate;
}

int ConfigManager::getNumPipelinedBuffer() const {
    assert (!_pt.empty());
    return _buffer.numPipelinedBuffer;
}

bool ConfigManager::usePipelinedBuffer() const {
    assert (!_pt.empty());
    return _buffer.numPipelinedBuffer > 1;
}

int ConfigManager::getHotnessLevel() const {
    assert (!_pt.empty());
    return _hotness.levels;
}

bool ConfigManager::useSlave() const {
    assert (!_pt.empty());
    return _hotness.useSlave;
}

len_t ConfigManager::getColdStorageCapacity() const {
    assert (!_pt.empty());
    return _hotness.coldStorageSize;
}

std::string ConfigManager::getColdStorageDevice() const {
    assert (!_pt.empty());
    return _hotness.coldStorageDevice;
}

bool ConfigManager::useSeparateColdStorageDevice() const {
    assert (!_pt.empty());
    return !_hotness.coldStorageDevice.empty();
}

std::string ConfigManager::getLSMTreeDir() const {
    assert (!_pt.empty());
    return _key.lsmTreeDir;
}

segment_len_t ConfigManager::getKVLocationCacheSize() const {
    assert (!_pt.empty());
    return _key.locationCacheSize;
}

bool ConfigManager::dbNoCompress() const {
    assert (!_pt.empty());
    return _key.compress == false;
}

bool ConfigManager::persistLogMeta() const {
    assert (!_pt.empty());
    return _logmeta.persist;
}

uint32_t ConfigManager::getGreedyGCSize() const {
    assert (!_pt.empty());
    return _gc.greedyGCSize;
}

GCMode ConfigManager::getGCMode() const {
    assert (!_pt.empty());
    return _gc.mode;
}

uint32_t ConfigManager::getNumGCReadThread() const {
    assert (!_pt.empty());
    return _gc.numReadThread;
}

uint32_t ConfigManager::getMinValueSizeToLog() const {
    assert (!_pt.empty());
    return _kvsep.minValueSizeToLog;
}

bool ConfigManager::disableKvSeparation() const {
    assert (!_pt.empty());
    return _kvsep.disabled;
}

bool ConfigManager::enabledVLogMode() const {
    assert (!_pt.empty());
    return _vlog.enabled;
}

uint32_t ConfigManager::getVLogGCSize() const {
    assert (!_pt.empty());
    return _vlog.gcSize;
}

bool ConfigManager::enableCrashConsistency() const {
    assert (!_pt.empty());
    return _consistency.crash;
}

uint32_t ConfigManager::getHashTableDefaultSize() const {
    assert(!_pt.empty());
    return _misc.hashTableDefaultSize;
}

int ConfigManager::getHashTableDefaultHashMethod() const {
    assert(!_pt.empty());
    return _misc.hashMethod;
}

uint32_t ConfigManager::getNumParallelFlush() const {
    assert(!_pt.empty());
    return _misc.numParallelFlush;
}

uint32_t ConfigManager::getNumIOThread() const {
    assert(!_pt.empty());
    return _misc.numIoThread;
}

uint32_t ConfigManager::getNumCPUThread() const {
    assert(!_pt.empty());
    return _misc.numCPUThread;
}

bool ConfigManager::syncAfterWrite() const {
    assert(!_pt.empty());
    return _misc.syncAfterWrite;
}

uint32_t ConfigManager::getNumRangeScanThread() const {
    assert(!_pt.empty());
    return _misc.numRangeScanThread;
}

bool ConfigManager::enabledScanReadAhead() const {
    assert(!_pt.empty());
    return _misc.scanReadAhead;
}

len_t ConfigManager::getBatchWriteThreshold() const {
    assert(!_pt.empty());
    return _misc.batchWriteThreshold;
}

bool ConfigManager::useMmap() const {
    assert(!_pt.empty());
    return _misc.useMmap;
}

int ConfigManager::getMaxOpenFiles() const {
    assert(!_pt.empty());
    return _misc.maxOpenFiles;
}

DebugLevel ConfigManager::getDebugLevel() const {
    assert(!_pt.empty());
    return _debug.level;
}

void ConfigManager::printConfig() const {

    printf(
        "------- Basic -------\n"
        " Main segment size         : %lu B\n"
        " Log segment size          : %lu B\n"
        " Max. no. of main segment  : %lu  \n"
        " Max. no. of log segment   : %lu  \n"
        " Retry max.                  : %d   \n"
        " Store segments as files   : %s   \n"
        , getMainSegmentSize()
        , getLogSegmentSize()
        , getNumMainSegment()
        , getNumLogSegment()
        , getRetryMax()
        , segmentAsFile()? "true" : "false"
    );

    printf(
        "------- Buffer ------\n"
        " Update buffer size          : %lu\n"
        "  - Pipe depth               : %d\n"
        " In-place update             : %s\n"
        , getUpdateKVBufferSize()
        , getNumPipelinedBuffer()
        , isInPlaceUpdate()? "true" : "false"
    );
    printf(
        "------- Hotness -----\n"
        " Levels                      : %d\n"
        " Use cold storage            : %s\n"
        " Cold storage size           : %lu\n"
        " Cold storage device         : %s\n"
        , getHotnessLevel()
        , useSlave()? "true" : "false"
        , getColdStorageCapacity()
        , useSeparateColdStorageDevice()? getColdStorageDevice().c_str() : "(same, append)"
    );
    printf(
        "--------- GC --------\n"
        " Greedy size                 : %d\n"
        " Mode                        : %s\n"
        " Read threads                : %u\n"
        , getGreedyGCSize()
        , getGCMode() == ALL? "all" : 
          getGCMode() == LOG_ONLY? "selctive (ratio)" :
          "unknown"
        , getNumGCReadThread()
    );
    printf(
        "-------  Keys  ------\n"
        " Path to DB                  : %s\n"
        " DB Type                     : %s\n"
        " Cache size                  : %lu records\n"
        " Disable compression         : %s\n"
        "------ Log Meta -----\n"
        " Persist                     : %s\n"
        , getLSMTreeDir().c_str()
        , "LevelDB"
        , getKVLocationCacheSize()
        , dbNoCompress()? "true" : "false"
        , persistLogMeta()? "true" : "false"
    );
    printf(
        "--- KV-separation ---\n"
        " Disabled                    : %s\n"
        " Min. value size             : %u\n"
        , disableKvSeparation()? "true" : "false"
        , getMinValueSizeToLog()
    );
    printf(
        "--- VLog mode ---\n"
        " Enabled                     : %s\n"
        " GC size                     : %u\n"
        , enabledVLogMode()? "true" : "false"
        , getVLogGCSize()
    );
    printf(
        "---- Consistency ----\n"
        " Crash protection            : %s\n"
        , enableCrashConsistency()? "true" : "false"
    );
    printf(
        "-------- Misc -------\n"
        " Hash table default size     : %d records\n"
        " Hash table default hash     : %d\n"
        " Use direct IO               : %s\n"
        " Max. no. of I/O threads     : %d\n"
        " Max. no. of CPU threads     : %d\n"
        " No. of parallel segment   : %d\n"
        " Sync. after write           : %s\n"
        " Max. size for batched write : %lu\n"
        " No. of scan threads         : %u\n"
        " Use mmap                    : %s\n"
        "------- Debug  ------\n"
        " Debug Level                 : %d\n"
        , getHashTableDefaultSize()
        , getHashTableDefaultHashMethod()
#ifdef DISK_DIRECT_IO
        , "true"
#else // ifdef DISK_DIRECT_IO
        , "false"
#endif // ifdef DISK_DIRECT_IO
        , getNumIOThread()
        , getNumCPUThread()
        , getNumParallelFlush()
        , syncAfterWrite()? "true" : "false"
        , getBatchWriteThreshold()
        , getNumRangeScanThread()
        , useMmap()? "true" : "false"
        , (int) getDebugLevel()
    );
}
