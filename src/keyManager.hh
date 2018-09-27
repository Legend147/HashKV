#ifndef __KEY_MANAGER_HH__
#define __KEY_MANAGER_HH__

#include <vector>
#include <assert.h>
#include "define.hh"
#include "ds/keyvalue.hh"

class KeyManager {
public:
    class KeyIterator {
        public:
            KeyIterator() {};
            virtual ~KeyIterator() {};

            virtual bool isValid() {
                return false;
            }

            virtual void next() {
            }
            virtual void release() {
            }

            virtual std::string key() {
                return std::string();
            }

            virtual std::string value() {
                return std::string();
            }
    };

    KeyManager() {};
    virtual ~KeyManager() {};

    // interface to access keys and mappings to values
    virtual bool writeKey (char *keyStr, ValueLocation valueLoc, int needCache = 1) = 0;
    virtual bool writeKeyBatch (std::vector<char *> keys, std::vector<ValueLocation> valueLocs, int needCache = 1) = 0;
    virtual bool writeKeyBatch (std::vector<std::string> &keys, std::vector<ValueLocation> valueLocs, int needCache = 1) = 0;
    virtual ValueLocation getKey (const char *keyStr, bool checkExist = false) = 0;

    virtual bool writeMeta (const char *keyStr, int keySize, std::string metadata) = 0;
    virtual std::string getMeta (const char *keyStr, int keySize) = 0;

    virtual KeyManager::KeyIterator *getKeyIterator (char *keyStr) = 0;

    virtual void getKeys (char *startingKey, uint32_t n, std::vector<char*> &keys, std::vector<ValueLocation> &locs) = 0;
    virtual bool deleteKey (char *keyStr) = 0;

    // print
    virtual void printCacheUsage (FILE *out) = 0;
    virtual void printStats (FILE *out) = 0;

};

#endif /* __KEY_MANAGER_HH__ */
