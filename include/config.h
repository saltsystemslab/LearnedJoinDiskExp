#ifndef CONFIG_H
#define CONFIG_H

#include "slice.h"
#define PAGE_SIZE 4096
#define NUM_SORT_THREADS 8

#if USE_STRING_KEYS
typedef double PLR_SEGMENT_POINT;
#elif USE_INT_128
typedef unsigned __int128 PLR_SEGMENT_POINT;
#else
typedef uint64_t PLR_SEGMENT_POINT;

#endif

#endif
