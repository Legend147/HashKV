#include "lru.hh"

LruList::LruList() {
    this->init(DEFAULT_LRU_SIZE);
}

LruList::LruList(size_t listSize) {
    this->init(listSize);
}

LruList::~LruList() {
    if (_slots) 
        delete [] _slots;
}

void LruList::insert(unsigned char *key, segment_id_t segmentId) {
    struct LruListRecord *target = 0;

    if (key == 0) {
        assert(key != 0);
    }

    _lock.lock();

    auto it = _existingRecords.find(key);

    if (it != _existingRecords.end()) {
        // reuse existing record
        target = it->second;
        list_move(&target->listPtr, &_lruList);
    } else if (_freeRecords.next != &_freeRecords) {
        // allocate a new record
        target = segment_of(_freeRecords.next, LruListRecord, listPtr);
        list_move(&target->listPtr, &_lruList);
    } else {
        // replace the oldest record
        target = segment_of(_lruList.prev, LruListRecord, listPtr);
        list_move(&target->listPtr, &_lruList);
        _existingRecords.erase(target->key);
    }

    target->segmentId = segmentId;

    // mark the metadata as exists in LRU list
    if (it == _existingRecords.end()) {
        memcpy(target->key, key, KEY_SIZE);
        std::pair<unsigned char*, LruListRecord*> record(target->key, target);
        _existingRecords.insert(record);
    }

    _lock.unlock();
}

segment_id_t LruList::get (unsigned char *key) {
    segment_id_t ret = INVALID_SEGMENT;

    _lock.lock_shared();

    auto it = _existingRecords.find(key);

    if (it != _existingRecords.end()) {
        ret = it->second->segmentId;
    }

    _lock.unlock_shared();

    return ret;
}

bool LruList::update (unsigned char *key, segment_id_t segmentId) {
    bool found = false;

    if (key == 0) {
        assert(key != 0);
    }

    _lock.lock();

    auto it = _existingRecords.find(key);
    if (it != _existingRecords.end()) {
        it->second->segmentId = segmentId;
        found = true;
    }

    _lock.unlock();

    return found;
}

std::vector<std::string> LruList::getItems() {
    std::vector<std::string> list;

    _lock.lock_shared();

    struct list_head *rec;
    list_for_each_prev(rec, &_lruList) {
        list.push_back(std::string((char*) segment_of(rec, LruListRecord, listPtr)->key, KEY_SIZE));
    }

    _lock.unlock_shared();

    return list;
}

std::vector<std::string> LruList::getTopNItems(size_t n) {
    std::vector<std::string> list;

    _lock.lock();

    struct list_head *rec;
    list_for_each_prev(rec, &_lruList) {
        list.push_back(std::string((char*) segment_of(rec, LruListRecord, listPtr)->key, KEY_SIZE));
        if (list.size() > n)
            break;
    }

    _lock.unlock();

    return list;
}

bool LruList::removeItem(unsigned char *key) {
    bool exist = false;
    _lock.lock();

    if (key == 0) {
        assert(key != 0);
        return false;
    }

    auto it = _existingRecords.find(key);
    exist = it != _existingRecords.end();
    if (exist) {
        list_move(&it->second->listPtr, &_freeRecords);
        _existingRecords.erase(it);
    }
    _lock.unlock();

    return exist;
}

size_t LruList::getItemCount() {
    size_t count = 0;

    _lock.lock_shared();

    count = _existingRecords.size();

    _lock.unlock_shared();

    return count;
}

size_t LruList::getFreeItemCount() {
    size_t count = 0;

    _lock.lock_shared();

    count = _listSize - _existingRecords.size();

    _lock.unlock_shared();

    return count;
}

size_t LruList::getAndReset(std::vector<std::string>& dest, size_t n) {

    _lock.lock();

    struct list_head *rec, *savePtr;
    list_for_each_safe(rec, savePtr, &_lruList) {
        std::string key = std::string((char*) segment_of(rec, LruListRecord, listPtr)->key, KEY_SIZE);
        if (n == 0 || dest.size() < n) {
            dest.push_back(key);
        }
        _existingRecords.erase((unsigned char*) key.c_str());
        list_move(rec, &_freeRecords);
    }

    _lock.unlock();

    return dest.size();
}

void LruList::reset() {
    _lock.lock();

    struct list_head *rec, *savePtr;

    list_for_each_safe(rec, savePtr, &_lruList) {
        _existingRecords.erase(segment_of(rec, LruListRecord, listPtr)->key);
        list_move(rec, &_freeRecords);
    }

    _lock.unlock();
}

void LruList::init(size_t listSize) {
    _listSize = listSize;

    // init record
    INIT_LIST_HEAD(&_freeRecords);
    INIT_LIST_HEAD(&_lruList);
    _slots = new struct LruListRecord[ _listSize ];
    for (size_t i = 0; i < _listSize; i++) {
        INIT_LIST_HEAD(&_slots[ i ].listPtr);
        list_add(&_slots[ i ].listPtr, &_freeRecords);
    }

}

void LruList::print(FILE *output, bool countOnly) {
    struct list_head *rec;
    size_t i = 0;
    unsigned char *key;

    if (countOnly) 
        fprintf(output, "    ");

    fprintf(output, "Free: %lu; Used: %lu\n", this->getFreeItemCount(), this->getItemCount());

    if (countOnly) return;

    _lock.lock();

    list_for_each(rec, &_lruList) {
        key = segment_of(rec, LruListRecord, listPtr)->key;
        fprintf(output, "Record [%lu]: key = %.*s\n",
            i, KEY_SIZE, key 
        );
        i++;
    }

    _lock.unlock();
}

