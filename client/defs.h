#pragma once

#define USE_BTREE
#define nUSE_DB_CXX // use Berkeley DB
#define nPRINT_ROW // print each row for debug,

#ifdef _WIN32
#define WINDOWS
#endif

#define nUSE_THREAD
#define CACHE_SIZE (512*1024*1024)

#define IndexBlockSize 4096
