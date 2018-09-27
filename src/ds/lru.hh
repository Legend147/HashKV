#ifndef __SERVER_HOTNESS_LRU_HH__
#define __SERVER_HOTNESS_LRU_HH__

#include <string>
#include <unordered_map>
#include "../define.hh"
#include "keyvalue.hh"

extern "C" {
#include "list.hh"
}

struct LruListRecord {
    unsigned char key[KEY_SIZE];
    segment_id_t segmentId;
    struct list_head listPtr;
};

class LruList {
public:

    LruList();
    LruList(size_t listSize);
    ~LruList();

    /*
     * Insert a new key to the list
     *
     * @parm key pointer to an existing instance of key 
     * @parm segmentId cotnainer of the key-value pair 
     */
    void insert (unsigned char *key, segment_id_t segmentId);

    /*
     * Read a value by key
     *
     * @parm key pointer to an existing instance of key 
     */
    segment_id_t get (unsigned char *key);

    /*
     * Update the value by key
     *
     * @parm key point to an existing instance of key
     */
    bool update (unsigned char *key, segment_id_t segmentId);

    /*
     * Read all items in the list
     *
     * @return all items in the list
     */
    std::vector<std::string> getItems();

    /*
     * Read top N items in the list
     *
     * @parm n N
     * @return top N items in the list
     */
    std::vector<std::string> getTopNItems(size_t n);

    /*
     * Remove an item from the list
     *
     * @parm key the key to remove from list 
     * @return if there is any match for the key
     */
    bool removeItem(unsigned char* key);
    
    /*
     * Get number of items in the list
     *
     * @return number of items in the list 
     */

    size_t getItemCount();

    /*
     * Get number of items to insert before the list becomes full
     *
     * @return number of items to insert before the list becomes full
     */
    size_t getFreeItemCount();

    /*
     * Reset the list, i.e., clear all items
     */
    void reset();

    /*
     * Read top N items in the list, and reset the list
     *
     * @parm dest top N items in the list
     * @parm n N; 0 means to retrieve all items from the list
     * @return number of items retrieved
     */
    size_t getAndReset(std::vector<std::string> &dest, size_t n = 0);

    /*
     * Print the list
     * 
     * @parm output output file stream
     */
    void print(FILE *output = stdout, bool countOnly = false);

private:
    std::unordered_map<unsigned char*, LruListRecord*, hashKey, equalKey> _existingRecords;
    struct LruListRecord *_slots;
    struct list_head _freeRecords;
    struct list_head _lruList;

    size_t _listSize;

    RWMutex _lock;

    /*
     * Init the list
     * 
     * @parm listSize list size, i.e., max number of items in list
     */
    void init(size_t listSize);
};

#endif

