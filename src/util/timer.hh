#ifndef __UTIL__TIMER_HH__
#define __UTIL__TIMER_HH__

#include <ctime>
#include <chrono>

class Timer {
public:
    Timer() {
        _prev = 0;
    }

    ~Timer () {
    }

    void triggerTimer (bool start = true, const char *label = 0) {
        if (start) {
            _startTime = std::chrono::system_clock::now();
        } else {
            std::chrono::system_clock::time_point endTime = std::chrono::system_clock::now();
            printf("%s: %ld us\n", (label == 0? "NIL" : label), std::chrono::duration_cast<std::chrono::microseconds>(endTime - _startTime).count() + _prev);
        }
    }

    void startTimer() {
        _prev = 0;
        triggerTimer(true);
    }

    void stopTimer(const char *label) {
        triggerTimer(false, label);
    }

    void restartTimer() {
        triggerTimer(true);
    }

    void pauseTimer() {
        std::chrono::system_clock::time_point endTime = std::chrono::system_clock::now();
        _prev += std::chrono::duration_cast<std::chrono::microseconds>(endTime - _startTime).count();
    }

private:
    std::chrono::system_clock::time_point _startTime;
    size_t _prev;
};

#endif //define __UTIL__TIMER_HH__
