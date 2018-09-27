#ifndef __MAXHEAP_HH__
#define __MAXHEAP_HH__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

template<class T> class MaxHeap {
public:
    MaxHeap(int maxNumStripe) {
        _value = (T*) malloc (sizeof(T) * maxNumStripe);
        _heap = (int*) malloc (sizeof(int) * maxNumStripe);
        _ps = (int*) malloc (sizeof(int) * maxNumStripe);
        memset(this->_value, 0, sizeof(int) * maxNumStripe);
        memset(this->_ps, 0, sizeof(int) * maxNumStripe);
        _heaplen = 0;
        _size = maxNumStripe;
    }

    ~MaxHeap() {
        free(_value);
        free(_heap);
        free(_ps);
    }

    void update(int r, T total, T delta) {
        if (r < 0 || r >= _size) {
            return;
        }
        _lock.lock();
        if (_ps[r] == -1) {
            _lock.unlock();
            return;
        }
        if (_ps[r] == 0) { // newly added, assume to be fully filled (?)
            _heap[++_heaplen] = r;
            _ps[r] = _heaplen;
            _value[r] = total;
        }
        _value[r] += delta;
        int q = _ps[r], p = q >> 1;
        // bubble up
        while (p && _value[_heap[p]] < _value[r]) {
            _ps[_heap[p]] = q;
            _heap[q] = _heap[p];
            q = p;
            p = q >> 1;
        }
        _heap[q] = r;
        _ps[r] = q;
        _lock.unlock();
    }

    std::pair<int, T> getMax() {
        _lock.lock();
        int max = -1;
        if (_heaplen == 0) {
            _lock.unlock();
            std::pair<int, T> ret;
            ret.first = -1;
            return ret;
        }
        max = _heap[1];
        int p = 1, q = 2, r = _heap[_heaplen--];
        // reorder the heap such that next max is at heap[1]
        while (q <= _heaplen) {
            if (q < _heaplen && _value[_heap[q+1]] > _value[_heap[q]]) q++;
            if (_value[_heap[q]] > _value[r]) {
                _ps[_heap[q]] = p; _heap[p] = _heap[q];
                p = q; q = p << 1;
            } else {
                break;
            }
        }
        _heap[p] = r; _ps[r] = p;

        std::pair<int, T> ret (max ,_value[max]);

        // this max is taken away
        //_ps[max] = -1;
        _ps[max] = 0;           // allow reuse of same index 
        _value[max] = 0;        // reset the value

        _lock.unlock();

        return ret;
    }

    T getValue(int r) { 
        if (r < 0 || r >= _size) {
            return 0;
        }
        _lock.lock();
        T value = (_ps[r] == 0 || _ps[r] == -1)? 0 : _value[r];
        _lock.unlock();
        return value;
    }

    void print(FILE *out = stderr) {
        _lock.lock_shared();
        for (int i = 1; i <= _heaplen; i++) {
            fprintf(out, "heap[%d] = %d, value = %llu, ps = %d\n", i, _heap[i], (unsigned long long)_value[_heap[i]], _ps[_heap[i]]);
        }
        _lock.unlock_shared();
    }

private:
    RWMutex _lock;
    T   *_value;    // the value of the item
    int *_heap;     // mark the item id
    int *_ps;       // mark the pos of item in heap
    int _heaplen;
    int _size;
};

#endif // ifndef __MAXHEAP_HH__
