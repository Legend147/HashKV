#ifndef __KVSERVER_HH__
#define __KVSERVER_HH__

#include <atomic>
#include <vector>
#include "define.hh"
#include "deviceManager.hh"
#include "keyManager.hh"
#include "valueManager.hh"
#include "segmentGroupManager.hh"
#include "logManager.hh"

/**
 * KvServer -- Interface for applications
 */
class KvServer {
public:
    KvServer();
    KvServer(DeviceManager *deviceManager);
    ~KvServer();

    bool putValue (char *key, len_t keySize, char *value, len_t valueSize);
    bool getValue (char *key, len_t keySize, char *&value, len_t &valueSize, bool timed = true);
    void getRangeValues(char *startingKey, uint32_t numKeys, std::vector<char*> &keys, std::vector<char*> &values, std::vector<len_t> &valueSize);
    bool delValue (char *key, len_t keySize);
    
    bool flushBuffer();
    size_t gc(bool all = false);

    void printStorageUsage(FILE *out = stdout);
    void printGroups(FILE *out = stdout);
    void printBufferUsage(FILE *out = stdout);
    void printKeyCacheUsage(FILE *out = stdout);
    void printKeyStats(FILE *out = stdout);
    void printValueSlaveStats(FILE *out = stdout);
    void printGCStats(FILE *out = stdout);

private:
    KeyManager *_keyManager;
    ValueManager *_valueManager;
    DeviceManager *_deviceManager;
    LogManager *_logManager;
    GCManager *_gcManager;
    SegmentGroupManager *_segmentGroupManager;

    boost::threadpool::pool _scanthreads;

    bool _freeDeviceManager; 
    bool checkKeySize(len_t &keySize);

    void getValueMt(char *key, len_t keySize, char *&value, len_t &valueSize, ValueLocation valueLoc, uint8_t &ret, std::atomic<size_t> &keysInProcess);
};
#endif
