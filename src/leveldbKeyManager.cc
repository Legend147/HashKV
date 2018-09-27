#include "leveldbKeyManager.hh"
#include "leveldb/write_batch.h"
#include "statsRecorder.hh"

LevelDBKeyManager::LevelDBKeyManager(const char *fs) {
    // init db
    leveldb::Options options;
    options.create_if_missing = true;
    options.compression = ConfigManager::getInstance().dbNoCompress() ? leveldb::CompressionType::kNoCompression : leveldb::CompressionType::kSnappyCompression;
    options.max_open_files = ConfigManager::getInstance().getMaxOpenFiles();
    leveldb::Status status = leveldb::DB::Open(options, fs, &_lsm);
    // report error if fails to open leveldb
    if(!status.ok()) {
        fprintf(stderr, "Error on DB open %s\n", status.ToString().c_str());
        assert(status.ok());
    }

    // init cache
    _cache = {0};
    int cacheSize = ConfigManager::getInstance().getKVLocationCacheSize();
    if (cacheSize) {
        _cache.lru = new LruList(cacheSize);
    }
}

LevelDBKeyManager::~LevelDBKeyManager() {
    if (_cache.lru != 0)
        delete _cache.lru;
    delete _lsm;
}

bool LevelDBKeyManager::writeKey (char *keyStr, ValueLocation valueLoc, int needCache) {
    bool ret = false;

    // update cache
    if (_cache.lru && needCache) {
        unsigned char* key = (unsigned char*) keyStr;
        if (needCache > 1) {
            STAT_TIME_PROCESS(_cache.lru->update(key, valueLoc.segmentId), StatsType::KEY_UPDATE_CACHE);
        } else {
            STAT_TIME_PROCESS(_cache.lru->insert(key, valueLoc.segmentId), StatsType::KEY_SET_CACHE);
        }
    }

    leveldb::WriteOptions wopt;
    wopt.sync = ConfigManager::getInstance().syncAfterWrite();

    // put the key into LSM-tree
    STAT_TIME_PROCESS(ret = _lsm->Put(wopt, leveldb::Slice(keyStr, KEY_SIZE), leveldb::Slice(valueLoc.serialize())).ok(), KEY_SET_LSM);
    return ret;
}

bool LevelDBKeyManager::writeKeyBatch (std::vector<char *> keys, std::vector<ValueLocation> valueLocs, int needCache) {
    bool ret = true;
    assert(keys.size() == valueLocs.size());
    if (keys.empty())
        return ret;

    // update to LSM-tree
    leveldb::WriteBatch batch;
    for (size_t i = 0; i < keys.size(); i++) {
        // update cache if needed
        if (_cache.lru && needCache) {
            if (needCache > 1) {
                STAT_TIME_PROCESS(_cache.lru->update((unsigned char*) keys.at(i), valueLocs.at(i).segmentId), StatsType::KEY_UPDATE_CACHE);
            } else {
                STAT_TIME_PROCESS(_cache.lru->insert((unsigned char*) keys.at(i), valueLocs.at(i).segmentId), StatsType::KEY_SET_CACHE);
            }
        }
        // construct the batch for LSM-tree write
        batch.Put(leveldb::Slice(keys.at(i), KEY_SIZE), leveldb::Slice(valueLocs.at(i).serialize()));

    }

    leveldb::WriteOptions wopt;
    wopt.sync = ConfigManager::getInstance().syncAfterWrite();

    // put the keys into LSM-tree
    STAT_TIME_PROCESS(ret = _lsm->Write(wopt, &batch).ok(), StatsType::KEY_SET_LSM_BATCH);
    return ret;
}

bool LevelDBKeyManager::writeKeyBatch (std::vector<std::string> &keys, std::vector<ValueLocation> valueLocs, int needCache) {
    bool ret = true;
    assert(keys.size() == valueLocs.size());
    if (keys.empty())
        return ret;

    // update to LSM-tree
    leveldb::WriteBatch batch;
    for (size_t i = 0; i < keys.size(); i++) {
        // update cache if needed
        if (_cache.lru && needCache) {
            if (needCache > 1) {
                STAT_TIME_PROCESS(_cache.lru->update((unsigned char*) keys.at(i).c_str(), valueLocs.at(i).segmentId), StatsType::KEY_UPDATE_CACHE);
            } else {
                STAT_TIME_PROCESS(_cache.lru->insert((unsigned char*) keys.at(i).c_str(), valueLocs.at(i).segmentId), StatsType::KEY_SET_CACHE);
            }
        }
        // construct the batch for LSM-tree write
        batch.Put(leveldb::Slice(keys.at(i)), leveldb::Slice(valueLocs.at(i).serialize()));

    }

    leveldb::WriteOptions wopt;
    wopt.sync = ConfigManager::getInstance().syncAfterWrite();

    // put the keys into LSM-tree
    STAT_TIME_PROCESS(ret = _lsm->Write(wopt, &batch).ok(), StatsType::KEY_SET_LSM_BATCH);
    return ret;
}

bool LevelDBKeyManager::writeMeta (const char *keyStr, int keySize, std::string metadata) {
    leveldb::WriteOptions wopt;
    wopt.sync = ConfigManager::getInstance().syncAfterWrite();
    //printf("META %.*s %s\n", keySize, keyStr, metadata.c_str());

    return _lsm->Put(wopt, leveldb::Slice(keyStr, keySize), leveldb::Slice(metadata)).ok();
}

std::string LevelDBKeyManager::getMeta (const char *keyStr, int keySize) {
    std::string value;

    _lsm->Get(leveldb::ReadOptions(), leveldb::Slice(keyStr, keySize), &value);

    return value;
}

ValueLocation LevelDBKeyManager::getKey (const char *keyStr, bool checkExist) {
    std::string value;
    ValueLocation valueLoc;
    valueLoc.segmentId = INVALID_SEGMENT;
    // only use cache for checking before SET
    if (checkExist && _cache.lru /* cache enabled */) {
        STAT_TIME_PROCESS(valueLoc.segmentId = _cache.lru->get((unsigned char*) keyStr), StatsType::KEY_GET_CACHE);
    }
    // if not in cache, search in the LSM-tree
    if (valueLoc.segmentId == INVALID_SEGMENT) {
        leveldb::Slice key (keyStr, KEY_SIZE);
        leveldb::Status status;
        STAT_TIME_PROCESS(status = _lsm->Get(leveldb::ReadOptions(), key, &value), StatsType::KEY_GET_LSM);
        // value location found
        if (status.ok()) {
            valueLoc.deserialize(value);
        }
    }
    return valueLoc;
}

void LevelDBKeyManager::getKeys (char *startingKey, uint32_t n, std::vector<char*> &keys, std::vector<ValueLocation> &locs) {
    // use the iterator to find the range of keys
    leveldb::Iterator *it = _lsm->NewIterator(leveldb::ReadOptions());
    it->Seek(leveldb::Slice(startingKey));
    ValueLocation loc;
    char *key = 0;
    for (uint32_t i = 0; i < n && it->Valid(); i++, it->Next()) {
        key = new char[KEY_SIZE];
        memcpy(key, it->key().ToString().c_str(), KEY_SIZE);
        //printf("FIND (%u of %u) [%0x][%0x][%0x][%0x]\n", i, n, key[0], key[1], key[2], key[3]);
        keys.push_back(key);
        loc.deserialize(it->value().ToString());
        locs.push_back(loc);
    }
    delete it;
}

LevelDBKeyManager::LevelDBKeyIterator *LevelDBKeyManager::getKeyIterator (char *startingKey) {
    leveldb::Iterator *it = _lsm->NewIterator(leveldb::ReadOptions());
    it->Seek(leveldb::Slice(startingKey));

    LevelDBKeyManager::LevelDBKeyIterator *kit = new LevelDBKeyManager::LevelDBKeyIterator(it);
    return kit;
}

bool LevelDBKeyManager::deleteKey (char *keyStr) {
    // remove the key from cache
    if (_cache.lru) {
        _cache.lru->removeItem((unsigned char*) keyStr);
    }

    leveldb::WriteOptions wopt;
    wopt.sync = ConfigManager::getInstance().syncAfterWrite();


    // remove the key from LSM-tree
    return _lsm->Delete(wopt, leveldb::Slice(keyStr, KEY_SIZE)).ok();
}


void LevelDBKeyManager::printCacheUsage(FILE *out) {
    fprintf(out, 
            "Cache Usage (KeyManager):\n"
    );
    if (_cache.lru) {
        fprintf(out, 
                " LRU\n"
        );
        _cache.lru->print(out, true);
    }
    fprintf(out, 
            " Shadow Hash Table\n"
    );
}

void LevelDBKeyManager::printStats(FILE *out) {
    std::string stats;
    _lsm->GetProperty("leveldb.stats", &stats);
    fprintf(out,
            "LSM (KeyManager):\n"
            "%s\n"
            , stats.c_str()
    );
}
