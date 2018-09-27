#ifndef __MINHEAP_HH__
#define __MINHEAP_HH__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

template<class T> class MinHeap {
public:
    MinHeap(int maxNumStripe) {
        _value = (T*) malloc (sizeof(T) * maxNumStripe);
        _heap = (int*) malloc (sizeof(int) * maxNumStripe);
        _ps = (int*) malloc (sizeof(int) * maxNumStripe);
        memset(this->_value, 0, sizeof(int) * maxNumStripe);
        memset(this->_ps, 0, sizeof(int) * maxNumStripe);
        _heaplen = 0;
        _size = maxNumStripe;
    }

    ~MinHeap() {
        free(_value);
        free(_heap);
        free(_ps);
    }

    void update(int r, T total, T delta, bool newValue = false) {
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
        if (newValue) {
            _value[r] = delta;
        } else {
            _value[r] += delta;
        }
        int q = _ps[r], p = q >> 1;
        // bubble up
        while (p && _value[_heap[p]] > _value[r]) {
            _ps[_heap[p]] = q;
            _heap[q] = _heap[p];
            q = p;
            p = q >> 1;
        }
        _heap[q] = r;
        _ps[r] = q;
        _lock.unlock();
    }

    std::pair<int, T> getMin() {
        _lock.lock();
        int min = -1;
        if (_heaplen == 0) {
            _lock.unlock();
            std::pair<int, T> ret;
            ret.first = -1;
            return ret;
        }
        min = _heap[1];
        int p = 1, q = 2, r = _heap[_heaplen--];
        // reorder the heap such that next min is at heap[1]
        while (q <= _heaplen) {
            if (q < _heaplen && _value[_heap[q+1]] < _value[_heap[q]]) q++;
            if (_value[_heap[q]] < _value[r]) {
                _ps[_heap[q]] = p; _heap[p] = _heap[q];
                p = q; q = p << 1;
            } else {
                break;
            }
        }
        _heap[p] = r; _ps[r] = p;

        std::pair<int, T> ret (min,_value[min]);

        // this min is taken away
        //_ps[min] = -1;
        _ps[min] = 0;
        _value[min] = 0;

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

    void print() {
        _lock.lock_shared();
        for (int i = 1; i <= _heaplen; i++) {
            fprintf(stderr, "heap[%d] = %d, value = %llu, ps = %d\n", i, _heap[i], (unsigned long long)_value[_heap[i]], _ps[_heap[i]]);
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

#endif // ifndef __MINHEAP_HH__
