#pragma once

#define USE_DB_CXX // use Berkeley DB
#define nPRINT_ROW // print each row for debug,

#ifdef _WIN32
#define WINDOWS
#endif

#define USE_THREAD
#define CACHE_SIZE (128*1024*1024)
