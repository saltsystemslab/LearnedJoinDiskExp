#ifndef CONFIG_H
#define CONFIG_H

#include "slice.h"
#define PAGE_SIZE 4096
#define NUM_SORT_THREADS 8

#if USE_LONG_DOUBLE
typedef long double PLR_SEGMENT_POINT;
#else
typedef double PLR_SEGMENT_POINT;
#endif

#endif
