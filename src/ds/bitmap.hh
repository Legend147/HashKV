#ifndef __BITMAP_HH__
#define __BITMAP_HH__

#include <mutex>
#include <vector>

#include "define.hh"

using namespace std;

class BitMap {
public:
    /**
     * Constructor
     */
    BitMap();

    /**
     * Constructor
     * \param size the size of the bitmap
     */
    BitMap(offset_t size);

    /**
     * Destructor
     */
    ~BitMap();

    /**
     * getBit
     * Return the bit corresponding to the address
     * \param addr the address 
     * \return the bit representing the address
     */
    bool getBit(offset_t addr);

    /**
     * setBit
     * Set the bit corresponding to the address to 1
     * \param addr the address 
     */
    void setBit(offset_t addr);

    /**
     * setBitRange
     * Set the bits corresponding to the address range to 1
     * \param addr the address 
     * \param n number of addresses in a roll
     */
    void setBitRange(offset_t addr, offset_t n);

    /**
     * clearBit
     * Set the bit corresponding to the address to 0
     * \param addr the address 
     */
    void clearBit(offset_t addr);

    /**
     * setAllOne
     * Set all the bits in bitmap to 1
     */
    void setAllOne();

    /**
     * getFirstZeroAndFlip
     * Search the first 0 from writeFront and set it to 1
     * \param writeFront the first addr to search (inclusive)
     * \return the address of the bit set to 1, -1 means no bits can be set
     */
    offset_t getFirstZeroAndFlip(offset_t writeFront = 0);

    /**
     * getFirstZeroAndFlip
     * Search the first sequence of 0 of len from writeFront and set them to 1
     * \param writeFront the first addr to search (inclusive)
     * \param len the length of the sequence required to set into 1
     * \return the starting address of the bit set to 1, 
     * -1 means no bits can be set
     */
    offset_t getFirstZerosAndFlip(offset_t writeFront = 0, offset_t len = 1);

    /**
     * getFirstZeroSince
     * Search the first 0 from addr in the bitmap
     * \param addr the first addr to search (inclusive)
     * \return the first address of the bit 0, -1 means no 0 is found
     */
    offset_t getFirstZeroSince(offset_t addr);

    /**
     * clearBitRange
     * Set the n bits from addr_st to 0
     * \param addr_st the starting address for bit setting (inclusive)
     * \param n the number of bits to set
     */
    void clearBitRange(offset_t addr_st, int n);

    /**
     * getAllOne
     * Get all bit positions that are set to 1
     * \return a list of (starting address, length) pair indicating the 
     * ranges of bit 1
     */
    vector<off_len_t> getAllOne();

private:
    ULL* _bv;                              /** < the bitmap*/
    offset_t _size;                               /** < the size of bitmap*/
    offset_t _num;                                /** < the number of internal bitmap units */

    static const int _unit;                /** < the size of a bitmap unit */
    static const ULL _onemask;             /** < the "all 1" bitmap unit mask */
    static const ULL _bitmask[];           /** < the bitmap mask array for setting 1s*/
    static const ULL _clearmask[];         /** < the bitmap mask array for setting 0s*/

    
    std::mutex* _bitmapMutex;                   /** < internal lock for bitmap operations */

    void clearBitInternal(offset_t addr);
};

#endif /* BITMAP_HH */
