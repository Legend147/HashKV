# HashKV

Enabling Efficient Updates in Key-value Storage via Hashing


## Overview

The prototype is written in C++ and uses 3rd parity libraries, including
  - [Boost library](http://www.boost.org/)
  - [HdrHistogram_c](https://github.com/HdrHistogram/HdrHistogram_c)
  - [LevelDB](https://github.com/google/leveldb)
  - [Snappy](https://github.com/google/snappy)
  - [Threadpool](http://threadpool.sourceforge.net/)


### Minimal Requirements

Minimal requirement to test the prototype:
  - Ubuntu 14.04 LTS (Server)
  - 1GB RAM


## Installation

1. On Ubuntu 14.04 LTS (Server), install 
   - C++ compiler: `g++` (version 4.8.4 or above)
   - Boost library: `libboost-system-dev`, `libboost-filesystem-dev`, `libboost-thread-dev`
   - Snappy: `libsnappy-dev`
   - CMake (required to compile HdrHistogram_c): `cmake`
   - Zlib (required to compile HdrHistogram_c): `zlib1g-dev`

```Shell
$ sudo apt-get update
$ sudo apt-get install g++ libboost-system-dev libboost-filesystem-dev libboost-thread-dev libsnappy-dev cmake zlib1g-dev
```

2. Download and extract the source code tarball (./hashkv-*.tar.gz)

```Shell
$ tar zxf hashkv-*.tar.gz
```

3. Setup the environment variable for HashKV as the root directory of folder `hashkv`

```Shell
$ cd hashkv
$ export HASHKV_HOME=$(pwd)
```

4. Compile HdrHistogram\_c (`libhdr_histogram.so`) under `lib/HdrHistogram_c-0.9.4`,

```Shell
$ cd ${HASHKV_HOME}/lib/HdrHistogram_c-0.9.4
$ cmake .
$ make
```

5. Compile LevelDB (`libleveldb.so`) under `lib/leveldb`,

```Shell
$ cd ${HASHKV_HOME}/lib/leveldb
$ make
```

### Testing the Prototype

1. Compile the prototype and the test program.

```Shell
$ cd ${HASHKV_HOME}
$ make
```
The test program is generated under `bin/` after compilation, named `hashkv_test`.

2. Before running, add path to shared libraries under `lib/leveldb/out-shared` and `lib/HdrHistogram_c-0.9.4/src`:

```Shell
$ export LD_LIBRARY_PATH="$HASHKV_HOME/lib/leveldb/out-shared:$HASHKV_HOME/lib/HdrHistogram_c-0.9.4/src:$LD_LIBRARY_PATH"
```

3. Then, switch to folder `bin/`

```Shell
$ cd ${HASHKV_HOME}/bin
```

4. Create the folder for key storage (LSM-tree), which is named `leveldb` by default

```Shell
$ mkdir leveldb
```

5. Create the folder for value storage

```Shell
$ mkdir data_dir
```

6. Clean the LSM-tree and the value storage folders before run

```Shell
$ rm -f data_dir/* leveldb/*
```

7. Choose and copy one of the example configuration files `[db_sample_config.ini]` corresponding to different designs
  - HashKV: `hashkv_sample_config.ini`
  - vLog: `vlog_sample_config.ini`
  - LevelDB: `leveldb_sample_config.ini`

```Shell
$ cp [db_sample_config.ini] config.ini
```

8. Run the test program of the prototype under the chosen design

```Shell
$ ./hashkv_test data_dir 100000
```

You can repeat steps 6-8 to try other designs. 

*Note that since the data layout differs between designs, the LSM-tree and the value store folders must be cleared when one switches to another design.*


## Publications

See our papers for greater design details of HashKV:

  - Helen H. W. Chan, Yongkun Li, Patrick P. C. Lee, and Yinlong Xu. [HashKV: Enabling Efficient Updates in KV Storage via Hashing][atc18hashkv] (USENIX ATC 2018)


[atc18hashkv]: https://www.usenix.org/conference/atc18/presentation/chan
