#ifndef __KEYVALUE_HH__
#define __KEYVALUE_HH__

#include <stdlib.h>
#include "../util/hash.hh"
#include "../define.hh"
#include "../configManager.hh"

#define LSM_MASK (0x80000000)

struct equalKey {
    bool operator() (unsigned char* const &a, unsigned char* const &b) const {
        return (memcmp(a, b, KEY_SIZE) == 0); 
    }
};  

struct hashKey {
    size_t operator() (unsigned char* const &s) const {
        return HashFunc::hash((char*) s, KEY_SIZE);
        //return std::hash<unsigned char>{}(*s);
    }
};  


class ValueLocation {
public:
    segment_len_t length;
    segment_id_t segmentId;
    segment_offset_t offset;
    std::string value;

    ValueLocation() {
        segmentId = INVALID_SEGMENT;
        offset = INVALID_OFFSET;
        length = INVALID_LEN;
    }

    ~ValueLocation() { }

    static unsigned int size() {
        return sizeof(segment_id_t) + sizeof(segment_offset_t) + sizeof(segment_len_t);
    }

    std::string serialize() {
        bool disableKvSep = ConfigManager::getInstance().disableKvSeparation();
        bool vlog = ConfigManager::getInstance().enabledVLogMode();
        len_t mainSegmentSize = ConfigManager::getInstance().getMainSegmentSize();
        len_t logSegmentSize = ConfigManager::getInstance().getLogSegmentSize();
        segment_id_t numMainSegment = ConfigManager::getInstance().getNumMainSegment();

        std::string str;
        // write length
        len_t flength = this->length;
        if (this->segmentId == LSM_SEGMENT) {
            flength |= LSM_MASK;
        }
        str.append((char*) &flength, sizeof(this->length));
        // write segment id (if kv-separation is enabled)
        offset_t foffset = this->offset;
        if (!disableKvSep && !vlog) {
            if (this->segmentId == LSM_SEGMENT) {
                foffset = INVALID_OFFSET;
            } else if (this->segmentId < numMainSegment) {
                foffset = this->segmentId * mainSegmentSize + this->offset;
            } else {
                foffset = mainSegmentSize * numMainSegment + (this->segmentId - numMainSegment) * logSegmentSize + this->offset;
            }
            //str.append((char*) &this->segmentId, sizeof(this->segmentId));
        } else if (!vlog) {
            this->segmentId = LSM_SEGMENT;
        }
        // write value or offset
        if (this->segmentId == LSM_SEGMENT) {
            str.append(value);
        } else {
            str.append((char*) &foffset, sizeof(this->offset));
        }
        return str;
    }

    bool deserialize (std::string str) {
        bool disableKvSep = ConfigManager::getInstance().disableKvSeparation();
        bool vlog = ConfigManager::getInstance().enabledVLogMode();
        len_t mainSegmentSize = ConfigManager::getInstance().getMainSegmentSize();
        len_t logSegmentSize = ConfigManager::getInstance().getLogSegmentSize();
        segment_id_t numMainSegment = ConfigManager::getInstance().getNumMainSegment();
        segment_id_t numLogSegment = ConfigManager::getInstance().getNumLogSegment();

        const char *cstr = str.c_str();
        size_t offset = 0;
        // read length
        memcpy(&this->length, cstr + offset, sizeof(this->length));
        offset += sizeof(this->length);
        if (this->length & LSM_MASK) {
            this->segmentId = LSM_SEGMENT;
            this->length ^= LSM_MASK;
        }
        // read segment id (if kv-separation is enabled)
        if (!disableKvSep && !vlog) {
            //memcpy(&this->segmentId, cstr + offset, sizeof(this->segmentId));
            //offset += sizeof(this->segmentId);
        } else if (!vlog) {
            this->segmentId = LSM_SEGMENT;
        } else {
            this->segmentId = 0;
        }
        // read value or offset
        if (this->segmentId == LSM_SEGMENT) {
            value.assign(cstr + offset, this->length);
        } else {
            memcpy(&this->offset, cstr + offset, sizeof(this->offset));
            if (!vlog) {
                if (this->offset < numMainSegment * mainSegmentSize) {
                    this->segmentId = this->offset / mainSegmentSize;
                    this->offset %= mainSegmentSize;
                } else if (this->offset - mainSegmentSize * numMainSegment > logSegmentSize * numLogSegment) {
                        // appended cold storage
                        this->segmentId = numMainSegment + numLogSegment;
                        this->offset = this->offset - (mainSegmentSize * numMainSegment + numLogSegment * logSegmentSize);
                } else {
                    this->segmentId = (this->offset - mainSegmentSize * numMainSegment) / logSegmentSize;
                    this->offset = this->offset - (mainSegmentSize * numMainSegment + this->segmentId * logSegmentSize);
                    this->segmentId += numMainSegment;
                }
            }
        }
        return true;
    }

    inline bool operator==(ValueLocation &vl) {
        bool ret = false;
        if (
                (this->segmentId == LSM_SEGMENT && vl.segmentId == LSM_SEGMENT) ||
                ConfigManager::getInstance().disableKvSeparation()
        ) {
            ret = (this->length == vl.length &&
                this->value == vl.value);
        } else {
            ret = (this->segmentId == vl.segmentId &&
                this->offset == vl.offset &&
                this->length == vl.length);
        }
        return ret;
    }
};

#endif
