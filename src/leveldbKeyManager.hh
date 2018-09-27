#ifndef __LEVELDB_KEY_MANAGER_HH__
#define __LEVELDB_KEY_MANAGER_HH__

#include <unordered_map>
#include <unordered_set>
#include <cassert>
#include <string>
#include <vector>
#include <leveldb/db.h>
#include "keyManager.hh"
#include "ds/lru.hh"

#define LSM_KEY_MOD_INVALID_ID  (-1)

class LevelDBKeyManager : public KeyManager {
public:
    class LevelDBKeyIterator : public KeyIterator {
        public:
            LevelDBKeyIterator(leveldb::Iterator *it) {
                _it = it;
            }

            ~LevelDBKeyIterator() {
                release();
            }

            virtual bool isValid() {
                if (_it == 0)
                    return false;
                return _it->Valid();
            }

            virtual void next() {
                if (_it == 0)
                    return;
                return _it->Next();
            }

            virtual void release() {
                delete _it;
                _it = 0;
            }

            virtual std::string key() {
                if (_it == 0)
                    return std::string();
                return _it->key().ToString();
            }

            virtual std::string value() {
                if (_it == 0)
                    return std::string();
                return _it->value().ToString();
            }

        private:
            leveldb::Iterator *_it;
    };

    LevelDBKeyManager(const char *fs);
    ~LevelDBKeyManager();

    // interface to access keys and mappings to values
    bool writeKey (char *keyStr, ValueLocation valueLoc, int needCache = 1);
    bool writeKeyBatch (std::vector<char *> keys, std::vector<ValueLocation> valueLocs, int needCache = 1);
    bool writeKeyBatch (std::vector<std::string> &keys, std::vector<ValueLocation> valueLocs, int needCache = 1);

    bool writeMeta (const char *keyStr, int keySize, std::string metadata);
    std::string getMeta (const char *keyStr, int keySize);

    ValueLocation getKey (const char *keyStr, bool checkExist = false);
    LevelDBKeyManager::LevelDBKeyIterator *getKeyIterator (char *keyStr);
    void getKeys (char *startingkey, uint32_t n, std::vector<char*> &keys, std::vector<ValueLocation> &locs);

    bool deleteKey (char *keyStr);
    void printCacheUsage (FILE *out);
    void printStats (FILE *out);

private:
    // lsm tree by leveldb
    leveldb::DB *_lsm;

    // cached locations of flushed keys
    struct {
        LruList *lru;
    } _cache;

};

#endif // ifndef __LEVELDB_KEY_MANAGER_HH__
