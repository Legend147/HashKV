#ifndef __SEGMENT_HH__
#define __SEGMENT_HH__

#include <vector>
#include <mutex>
#include "../util/debug.hh"
#include "../define.hh"
#include "../configManager.hh"
#include "list.hh"

class Segment {
public:
    Segment() {
        _id = INVALID_SEGMENT;
        _buf = 0;
        _length = INVALID_LEN;
        _writeFront = 0;
        _flushFront = 0;
    }

    // init a segment with buffer (re-)allocated
    static bool init(Segment &a, segment_id_t _id, segment_len_t size, bool needsSetZero = true) {
        assert(size > 0);

        if (a._length != size) {
            // free if previous allocated buffer does not fit
            ::free(a._buf);
            a._buf = 0;
            a._length = 0;
        }

        if (a._buf == 0) {
            // new buffer
            if (needsSetZero) {
                a._buf = (unsigned char*) buf_calloc(size, sizeof(unsigned char));
            } else {
                a._buf = (unsigned char*) buf_malloc(size);
            }
        } else if (needsSetZero) {
            // reuse and set zero
            memset(a._buf, 0, size);
        }

        reset(a);
        if (a._buf != 0) {
            a._id = _id;
            a._length = size;
        }

        debug_info("Malloc %lu for segment %lu\n", a._length, a._id);
        return (a._buf != 0 && a._id != INVALID_SEGMENT);
    }

    static bool dup(Segment &dest,  Segment &source) {
        bool ret = init(dest, source._id, source._length, false);
        dest._flushFront = source._flushFront;
        dest._writeFront = source._writeFront;
        if (ret) {
            memcpy(dest._buf, source._buf, source._length);
        }
        return ret;
    }

    // init a segment with buffer assigned
    static bool init(Segment &a, segment_id_t id, unsigned char* buf, segment_len_t size) {
        a._id = id;
        a._buf = buf;
        a._length = size;
        resetFronts(a);
        debug_info("Set length=%lu buffer=%p for segment %lu\n", a._length, a._buf, a._id);
        return true;
    }

    // reset the information of segment
    static void reset (Segment &a) {
        a._id = INVALID_SEGMENT;
        a._length = INVALID_LEN;
        resetFronts(a);
    }

    // reset the write frontier and flush frontier of segment
    static inline void resetFronts(Segment &a) {
        a._writeFront = 0;
        a._flushFront = 0;
    }

    // zero out (partial) segment and reset frontiers
    static inline void clean(Segment &a, segment_len_t start = 0, segment_len_t length = INVALID_LEN, bool resetFront = true) {
        if (a._length && a._buf) {
            if (start + length > a._length)
                return;
            if (length == INVALID_LEN)
                length = a._length;
            memset(a._buf + start, 0, length);
        }
        if (resetFront) resetFronts(a);
    }

    // assign ID to segment
    static inline void assignId(Segment &a, segment_id_t _id) { 
        a._id = _id;
    }

    // get ID of segment
    static inline segment_id_t getId(const Segment &a) {
        return a._id;
    }

    // get data buffer of segment
    static inline unsigned char *getData(const Segment &a) {
        return a._buf;
    }

    // append data to segment
    template<class T> static inline bool appendData(Segment &a, const T *buf, segment_len_t size) {
        if (!Segment::canFit(a, size) || a._buf == 0) {
            return false;
        }
        if (sizeof(T) > 1) {
            *(T*)(a._buf + a._writeFront) = *buf;
        } else {
            memcpy(a._buf + a._writeFront, buf, size);
        }
        a._writeFront += size;
        return true;
    }

    // in-place overwrite data in segment
    template<class T> static inline bool overwriteData(Segment &a, const T *buf, segment_off_len_t offlen) {
        if (offlen.first + offlen.second > a._length) {
            return false;
        }
        if (sizeof(T) > 1) {
            *(T*)(a._buf + offlen.first) = *buf ;
        } else {
            memcpy(a._buf + offlen.first, buf, offlen.second);
        }
        return true;
    }

    // read data from segment
    template<class T> static inline bool readData(Segment &a, T *buf, segment_off_len_t offLen, bool checkWriteFront = false) {
        if ((checkWriteFront && offLen.first + offLen.second > a._writeFront) || a._buf == 0) {
            return false;
        }
        if (sizeof(T) > 1) {
            assert(sizeof(T) == offLen.second);
            *buf = *(T*)(a._buf + offLen.first);
        } else {
            memcpy(buf, a._buf + offLen.first, offLen.second);
        }
        return true;
    }

    static inline segment_offset_t getWriteFront(const Segment &a) {
        return a._writeFront;
    }

    static inline bool setFlushFront(Segment &a, segment_offset_t flushFront) {
        assert(flushFront <= a._length && flushFront >= 0);
        if (flushFront > a._length || flushFront < 0)
            return false;
        a._flushFront = flushFront;
        if (a._writeFront < a._flushFront) {
            a._writeFront = a._flushFront;
        }
        return true;
    }

    static inline segment_offset_t getFlushFront(const Segment &a) {
        return a._flushFront;
    }

    static inline segment_len_t getSize(const Segment &a) {
        return a._length;
    }

    static inline segment_len_t getRemainingDataSize(const Segment &a) {
        return a._length - a._writeFront;
    }

    // check if segment is full
    static inline bool isFull(const Segment &a) {
        return (a._length > 0 && a._length <= a._writeFront);
    }

    // check if new data fits in
    static inline bool canFit(const Segment &a, segment_len_t size) {
        return (a._length != INVALID_LEN && a._length >= a._writeFront + size);
    }

    static inline bool setWriteFront(Segment &a, segment_offset_t writeFront) {
        if (writeFront > a._length)
            return false;
        a._writeFront = writeFront;
        return true;
    }

    // assume the segment is full of data
    static inline void setFull(Segment &a) {
        a._writeFront = a._length;
        a._flushFront = a._length;
    }

    // release the segment and resource in it 
    static void free(Segment &a) {
        ::free(a._buf);
        a._buf = 0;
        reset(a);
    }

    // dump information and data of segment
    static void dumpSegment(const Segment &a, bool allContent = false) {
        printf(
                "Segment _id=%lu "
                "_length=%lu "
                "wf=%lu\n",
                a._id,
                a._length,
                a._writeFront
        );
        unsigned char prev = 0;
        unsigned int start = 0;
        if (allContent) {
            for (unsigned int i = 0; i < a._length; i++) {
                if (i == 0) {
                    prev = a._buf[i];
                    continue;
                }
                if (prev != a._buf[i]) {
                    printf("%8u to %8u : [%08x]\n", start, i-1, prev);
                    start = i;
                    prev = a._buf[i];
                }
            }
            if (start < a._writeFront) {
                printf("%8u to %lu : [%08x]\n", start, a._writeFront-1, prev);
            }
            if (a._writeFront < a._length) {
                printf("%lu to %lu : [%08x]\n", a._writeFront, a._length-1, a._buf[a._writeFront]);
            }
        } else {
            printf("data\n");
            start = 0;
            for (unsigned int i = 0; i < a._writeFront; i++) {
                if (i == 0) {
                    prev = a._buf[i];
                    continue;
                }
                if (prev != a._buf[i]) {
                    printf("%8u to %8u : [%08x]\n", start, i-1, prev);
                    start = i;
                    prev = a._buf[i];
                }
            }
            if (start < a._writeFront) {
                printf("%8u to %lu : [%08x]\n", start, a._writeFront-1, prev);
            }
        }
    }

private:
    segment_id_t _id;                  // _id of a segment within a stripe
    segment_len_t _length;             // segment size
    segment_len_t _writeFront;         // write frontier
    segment_len_t _flushFront;         // flush frontier
    unsigned char *_buf;                 // data _buffer
};

struct SegmentBufferRecord {
    Segment segment;
    std::mutex *lock;
    union {
        hlist_node hnode;
        list_head node;
    };
};

typedef struct SegmentBufferRecord      SegmentBuffer;
typedef struct SegmentBufferRecord      SegmentRec;

#endif // __SEGMENT_HH__
