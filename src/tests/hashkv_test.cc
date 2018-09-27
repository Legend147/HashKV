#include "util/debug.hh"
#include "define.hh"
#include "kvServer.hh"
#include <ctime>
#include <chrono>
#include <stdlib.h>     // srand(), rand()
#include "../statsRecorder.hh"

#define VALUE_SIZE  (992)

#define VAR_SIZE (1)
#define KV_NUM_DEFAULT      (100 * 1000)
#define DISK_SIZE           (1024 * 1024 * 1024 * 1) // 1GB
#define UPDATE_RUNS (5)

#define GEN_VALUE(v, i, size)  \
    do { memset(v, 2+i, size); } while(0);

#define SKIPTHIS (ki % (ConfigManager::getInstance().getMainSegmentSize() / VALUE_SIZE) % 10 > 2)

#define DELTHIS (i % 11 < 2)

static int runCount = 0;
int KVNUM = KV_NUM_DEFAULT;
bool randkey = true;
int exitCode = 0;

std::unordered_map<int, std::string> loadKeys;

void gen_key (char *k, int i, int j) {
    char c;
    if (randkey == false) {
        c = 1 + i + j;
        if (c == 0) c++;
    } else {
        do {
            c = rand() % (sizeof(char) << 8);
        } while (c==0);
    }
    memset(k + j, c, 1);
}

void testSetKey(KvServer &kvserver, bool skipSome = false) {
    srand(123987);

    runCount += 1;

    int count = 0, failed = 0;
    char key[KEY_SIZE], value[VALUE_SIZE];
    std::unordered_set<int> usedKeys;
    for (int i = 0; i < KVNUM; i++) {
        int ki = -1;
        // always consume the key (in the random sequence)
        if (runCount == 1) {
            ki = i;
            for (int j = 0; j < KEY_SIZE; j++) {
                gen_key(key, i, j);
            }
            loadKeys[i] = std::string(key);
        } else {
            ki = i;
            memcpy(key, loadKeys.at(ki).c_str(), KEY_SIZE);
        }
        if (skipSome && SKIPTHIS) {
            continue;
        }
        GEN_VALUE(value, ki + runCount, VALUE_SIZE - ki % VAR_SIZE);
        bool ret = false;
        struct timeval start_time;
        gettimeofday(&start_time, 0);
        ret = kvserver.putValue(key, KEY_SIZE, value, VALUE_SIZE - ki % VAR_SIZE);
        StatsRecorder::getInstance()->timeProcess((runCount > 1 ? StatsType::UPDATE : StatsType::SET), start_time, /* diff = */ 0, /* count = */ 1, VALUE_SIZE - ki % VAR_SIZE);
        if (ret == false) {
            printf("Failed to set key %.*s\n", KEY_SIZE, key);
            failed++;
            exitCode = -1;
        }
        count++;
    } 

    printf(">>> Issued %d SET requests (%d failed)\n", count, failed);
}

void testReadBackKey(KvServer &kvserver, bool skipSome = false) {
    srand(123987);

    char key[KEY_SIZE], value[VALUE_SIZE], *readval;
    len_t valueSize;

    int failed = 0;
    for (int i = 0; i < KVNUM; i++) {
        int ki = i;
        for (int j = 0; j < KEY_SIZE; j++) {
            gen_key(key, i, j);
        }
        int count = (skipSome && SKIPTHIS)? 1 : runCount;
        GEN_VALUE(value, i + count, VALUE_SIZE - i % VAR_SIZE);
        readval = 0;
        struct timeval start_time;
        gettimeofday(&start_time, 0);
        kvserver.getValue(key, KEY_SIZE, readval, valueSize);
        StatsRecorder::getInstance()->timeProcess(StatsType::GET, start_time, /* diff = */ 0, /* count = */ 1, valueSize);
        if (readval == 0) {
            exitCode = -1;
            failed++;
        } else if (memcmp(value, readval, VALUE_SIZE - i % VAR_SIZE) != 0) {
            exitCode = -1;
            failed++;
            assert(0);
            delete readval;
        } else {
            delete readval;
        }
    } 
    printf(">>> Issued %d GET requests (%d failed)\n", KVNUM, failed);
    if (failed > 0) assert(0);
}

void reportUsage(KvServer &kvServer) {
    kvServer.printStorageUsage();
    kvServer.printBufferUsage();
    kvServer.printKeyCacheUsage();
    //kvServer.printGroups();
}

void timer (bool start = true, const char *label = 0) {
    static std::chrono::system_clock::time_point startTime;
    if (start) {
        startTime = std::chrono::system_clock::now();
    } else {
        std::chrono::system_clock::time_point endTime = std::chrono::system_clock::now();
        printf("Elapsed Time (%s): %.6lf s\n", label, std::chrono::duration_cast<std::chrono::microseconds>(endTime - startTime).count() / 1000.0 / 1000.0);
    }
}

void startTimer() {
    timer(true);
}

void stopTimer(const char *label) {
    timer(false, label);
}

int main(int argc, char **argv) {

    if (argc < 2) {
        fprintf(stderr, "%s <data directory> [num of KV pairs]\n", argv[0]);
        exit(-1);
    }

    // how many keys to generate
    if (argc >= 3) {
        KVNUM = atoi(argv[2]);
    }

    fprintf(stderr,"> Beginning of tests (KVNUM=%d)\n", KVNUM);
    ConfigManager::getInstance().setConfigPath("config.ini");

    // data disks
    DiskInfo disk1(0, argv[1], DISK_SIZE);

    std::vector<DiskInfo> disks;
    disks.push_back(disk1);

    DeviceManager diskManager(disks);

    KvServer kvserver(&diskManager);
    struct timeval startTime;
    gettimeofday(&startTime, 0);
    StatsRecorder::getInstance()->openStatistics(startTime);

    // write new keys and read-back
    print_yellow(">> Beginning of %s and read-back test", "simple fixed-size new write");
    startTimer();
    testSetKey(kvserver);
    stopTimer("SET");
    startTimer();
    testReadBackKey(kvserver);
    stopTimer("GET");
    print_green(">> End of %s and read-back test", "simple fixed-size new write");

    reportUsage(kvserver);

    // rewrite keys and read-back
    for (int i = 0; i < UPDATE_RUNS; i++) {
        print_yellow(">> Beginning of %s and read-back test (run %d)", "rewrite", i+1);
        startTimer();
        testSetKey(kvserver, true);
        stopTimer("UPDATE");
        testReadBackKey(kvserver, true);
        StatsRecorder::getInstance()->DestroyInstance();
        StatsRecorder::getInstance()->openStatistics(startTime);
        kvserver.printValueSlaveStats();
        kvserver.printGCStats();
        print_green(">> End of %s and read-back test (run %d)", "rewrite", i+1);
        reportUsage(kvserver);
    }

    reportUsage(kvserver);

    fprintf(stderr,"> End of update tests\n");

    return exitCode;
}
