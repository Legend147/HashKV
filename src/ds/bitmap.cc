#include "bitmap.hh"

#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include "../util/debug.hh"
#include "../configManager.hh"

const int BitMap::_unit = sizeof(ULL)<<3;

const ULL BitMap::_onemask = 0xFFFFFFFFFFFFFFFFULL;

const ULL BitMap::_bitmask[] = {
    0x0000000000000001ULL,0x0000000000000002ULL,0x0000000000000004ULL,0x0000000000000008ULL,
    0x0000000000000010ULL,0x0000000000000020ULL,0x0000000000000040ULL,0x0000000000000080ULL,
    0x0000000000000100ULL,0x0000000000000200ULL,0x0000000000000400ULL,0x0000000000000800ULL,
    0x0000000000001000ULL,0x0000000000002000ULL,0x0000000000004000ULL,0x0000000000008000ULL,
    0x0000000000010000ULL,0x0000000000020000ULL,0x0000000000040000ULL,0x0000000000080000ULL,
    0x0000000000100000ULL,0x0000000000200000ULL,0x0000000000400000ULL,0x0000000000800000ULL,
    0x0000000001000000ULL,0x0000000002000000ULL,0x0000000004000000ULL,0x0000000008000000ULL,
    0x0000000010000000ULL,0x0000000020000000ULL,0x0000000040000000ULL,0x0000000080000000ULL,
    0x0000000100000000ULL,0x0000000200000000ULL,0x0000000400000000ULL,0x0000000800000000ULL,
    0x0000001000000000ULL,0x0000002000000000ULL,0x0000004000000000ULL,0x0000008000000000ULL,
    0x0000010000000000ULL,0x0000020000000000ULL,0x0000040000000000ULL,0x0000080000000000ULL,
    0x0000100000000000ULL,0x0000200000000000ULL,0x0000400000000000ULL,0x0000800000000000ULL,
    0x0001000000000000ULL,0x0002000000000000ULL,0x0004000000000000ULL,0x0008000000000000ULL,
    0x0010000000000000ULL,0x0020000000000000ULL,0x0040000000000000ULL,0x0080000000000000ULL,
    0x0100000000000000ULL,0x0200000000000000ULL,0x0400000000000000ULL,0x0800000000000000ULL,
    0x1000000000000000ULL,0x2000000000000000ULL,0x4000000000000000ULL,0x8000000000000000ULL
};

const ULL BitMap::_clearmask[] = {
    0xFFFFFFFFFFFFFFFEULL,0xFFFFFFFFFFFFFFFDULL,0xFFFFFFFFFFFFFFFBULL,0xFFFFFFFFFFFFFFF7ULL,
    0xFFFFFFFFFFFFFFEFULL,0xFFFFFFFFFFFFFFDFULL,0xFFFFFFFFFFFFFFBFULL,0xFFFFFFFFFFFFFF7FULL,
    0xFFFFFFFFFFFFFEFFULL,0xFFFFFFFFFFFFFDFFULL,0xFFFFFFFFFFFFFBFFULL,0xFFFFFFFFFFFFF7FFULL,
    0xFFFFFFFFFFFFEFFFULL,0xFFFFFFFFFFFFDFFFULL,0xFFFFFFFFFFFFBFFFULL,0xFFFFFFFFFFFF7FFFULL,
    0xFFFFFFFFFFFEFFFFULL,0xFFFFFFFFFFFDFFFFULL,0xFFFFFFFFFFFBFFFFULL,0xFFFFFFFFFFF7FFFFULL,
    0xFFFFFFFFFFEFFFFFULL,0xFFFFFFFFFFDFFFFFULL,0xFFFFFFFFFFBFFFFFULL,0xFFFFFFFFFF7FFFFFULL,
    0xFFFFFFFFFEFFFFFFULL,0xFFFFFFFFFDFFFFFFULL,0xFFFFFFFFFBFFFFFFULL,0xFFFFFFFFF7FFFFFFULL,
    0xFFFFFFFFEFFFFFFFULL,0xFFFFFFFFDFFFFFFFULL,0xFFFFFFFFBFFFFFFFULL,0xFFFFFFFF7FFFFFFFULL,
    0xFFFFFFFEFFFFFFFFULL,0xFFFFFFFDFFFFFFFFULL,0xFFFFFFFBFFFFFFFFULL,0xFFFFFFF7FFFFFFFFULL,
    0xFFFFFFEFFFFFFFFFULL,0xFFFFFFDFFFFFFFFFULL,0xFFFFFFBFFFFFFFFFULL,0xFFFFFF7FFFFFFFFFULL,
    0xFFFFFEFFFFFFFFFFULL,0xFFFFFDFFFFFFFFFFULL,0xFFFFFBFFFFFFFFFFULL,0xFFFFF7FFFFFFFFFFULL,
    0xFFFFEFFFFFFFFFFFULL,0xFFFFDFFFFFFFFFFFULL,0xFFFFBFFFFFFFFFFFULL,0xFFFF7FFFFFFFFFFFULL,
    0xFFFEFFFFFFFFFFFFULL,0xFFFDFFFFFFFFFFFFULL,0xFFFBFFFFFFFFFFFFULL,0xFFF7FFFFFFFFFFFFULL,
    0xFFEFFFFFFFFFFFFFULL,0xFFDFFFFFFFFFFFFFULL,0xFFBFFFFFFFFFFFFFULL,0xFF7FFFFFFFFFFFFFULL,
    0xFEFFFFFFFFFFFFFFULL,0xFDFFFFFFFFFFFFFFULL,0xFBFFFFFFFFFFFFFFULL,0xF7FFFFFFFFFFFFFFULL,
    0xEFFFFFFFFFFFFFFFULL,0xDFFFFFFFFFFFFFFFULL,0xBFFFFFFFFFFFFFFFULL,0x7FFFFFFFFFFFFFFFULL
};

BitMap::BitMap(): BitMap(ConfigManager::getInstance().getNumSegment()) {
    debug("%s\n", "Constructing an default bitmap, using num segments");
}

BitMap::BitMap(offset_t size) {
    assert (!(_unit & (_unit-1))); // unit must be power of 2
    offset_t num = size / _unit;
    if (size % _unit) ++num;

    debug("Malloc %lu unit\n", num);
    _bv = (ULL*) malloc (sizeof(ULL) * num);

    assert(_bv);

    _num = num;
    _size = size;
    memset (_bv, 0, sizeof(ULL) * num);

    _bitmapMutex = new mutex();
}

BitMap::~BitMap() {
    free (_bv);
    delete _bitmapMutex;
}

bool BitMap::getBit(offset_t addr) {
    assert (addr < _size);
    return _bv[addr/_unit] & _bitmask[addr%_unit];
}

void BitMap::setBit(offset_t addr) {
    assert (addr < _size);
    _bv[addr/_unit] |= _bitmask[addr%_unit];
}

void BitMap::setBitRange(offset_t addr, offset_t n) {
    assert (addr + n < _size);
    for (offset_t cur = addr; cur < addr + n; cur++) {
        _bv[cur/_unit] |= _bitmask[cur%_unit];
    }
}

void BitMap::clearBit(offset_t addr) {
    lock_guard <mutex> lk(*_bitmapMutex);
    clearBitInternal(addr);
}

void BitMap::clearBitInternal(offset_t addr) {
    assert (addr < _size);
    _bv[addr/_unit] &= _clearmask[addr%_unit];
}

void BitMap::setAllOne() {
    lock_guard <mutex> lk(*_bitmapMutex);
    for (offset_t i = 0; i < _num; i++)
        _bv[i] = ~_bv[i];    
    if (_size % _unit) {
        _bv[_num-1] &= (1ULL << (_size % _unit)) - 1ULL;
    }
}

offset_t BitMap::getFirstZeroAndFlip(offset_t writeFront) {
    lock_guard <mutex> lk(*_bitmapMutex);
    offset_t ret = getFirstZeroSince(writeFront);
    if (ret != INVALID_OFFSET) setBit(ret);
    return ret;
}

offset_t BitMap::getFirstZerosAndFlip(offset_t writeFront, offset_t len) {
    lock_guard <mutex> lk(*_bitmapMutex);
    offset_t head = INVALID_OFFSET;
    bool reachEnd = false;
    for (offset_t i = 0; i < len; i++) {
        offset_t ret = getFirstZeroSince(writeFront+i);
        if (ret != INVALID_OFFSET && (ret == head+i || head == INVALID_OFFSET)) {
            setBit(ret);
            if (head == INVALID_OFFSET) {
                head = ret;
            }
        } else if (ret == INVALID_OFFSET) {
            // search twice before declaring no contiuous space for writing data
            if (reachEnd) {
                debug_error("Cannot alloc continuous blocks from map size %lu of len %lu (i=%lu), start %lu, last %lu, cwf = %lu\n", _size, len, i, head, ret, writeFront);
                exit(-1);
            } else {
                reachEnd = true;
                for (offset_t j = head; j < i + head; j++) {
                    //std::cerr << "Clear LBA " << j << std::endl;
                    clearBitInternal(j);
                }
                writeFront = 0;
                i = 0;
            }
        } else {
            debug_error("Cannot alloc continuous blocks of len %lu, start %lu, last %lu, i %lu", len, head, ret, i);
            for (offset_t j = head; j < i + head; j++) {
                //std::cerr << "Clear LBA " << j << std::endl;
                clearBitInternal(j);
            }
            writeFront = ret;
            setBit(ret);
            i = 0;
        }
        if (!i) head = ret;
    }
    return head;
}

offset_t BitMap::getFirstZeroSince(offset_t addr) {
    /* sequential search for now,
     * may switch to random or more advanced later
     * number of element in bitmap is expected to be
     * 128GB / 512 KB / 64 = 4K, should not take long
     */
    offset_t ret = INVALID_OFFSET;
    offset_t st = addr / _unit;
    ULL stmask = ~((1ULL << (addr % _unit)) - 1ULL);
    
    for (offset_t i = st; i < _num; i++) {
        ULL pos = _bv[i] ^ _onemask;
        if (i == st) pos &= stmask;
        if (pos) {
            ULL mask = _onemask;
            int depth = 0;
            int width = _unit;
            do {
                width >>= 1;
                mask >>= width;
                if (pos & mask) {
                    pos &= mask;
                } else {
                    pos = (pos >> width) & mask;
                    depth += width;
                }
            } while(width);
            ret = i * _unit + depth;
            break;
        }
    }
    if (ret >= _size) ret = -1;
    return ret;
}

void BitMap::clearBitRange(offset_t addr_st, int n) {
    lock_guard <mutex> lk(*_bitmapMutex);
    assert (n > 0);

    offset_t addr_ed = std::min(addr_st + n, _size) - 1;
    offset_t st = addr_st / _unit;
    offset_t ed = addr_ed / _unit;

    assert (st < _num && ed < _num);

    if (st == ed) {
        ULL mask = 0;
        if (addr_ed % _unit != _unit - 1)
            mask = ((1ULL << (_unit - 1 - addr_ed % _unit)) - 1ULL) << (addr_ed % _unit + 1);
        if (addr_st % _unit) 
            mask |= (1ULL << (addr_st % _unit)) - 1ULL;
        _bv[st] &= mask;
    } else {
        // first 
        ULL mask = 0;
        if (addr_st % _unit) 
            mask = (1ULL << (addr_st % _unit)) - 1ULL;
        _bv[st] &= mask;

        // middle
        for (offset_t i = st+1; i < ed; i++) _bv[i] = 0ULL;

        // last
        mask = 0;
        if (addr_ed % _unit != _unit - 1)
            mask = ((1ULL << (_unit - 1 - addr_ed % _unit)) - 1ULL) << (addr_ed % _unit + 1);
        _bv[ed] &= mask;
    }
}

vector<off_len_t> BitMap::getAllOne() {
    lock_guard <mutex> lk(*_bitmapMutex);
    vector<off_len_t> retv;
    offset_t off = 0;
    offset_t bit;
    while ((bit = getFirstZeroSince(off)) != INVALID_OFFSET) {
        if (bit != off) retv.push_back(make_pair(off, bit-off));
        off = bit+1;
    }
    if (off < _size) {
        retv.push_back(make_pair(off,_size-off));
    }
    return retv;
}
