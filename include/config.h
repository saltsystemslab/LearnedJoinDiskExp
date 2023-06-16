#ifndef CONFIG_H
#define CONFIG_H

#include "slice.h"

#if USE_STRING_KEYS

typedef Slice KEY_TYPE;
typedef double PLR_SEGMENT_POINT;

#elif USE_INT_128

typedef unsigned __int128 KEY_TYPE;
typedef unsigned __int128 PLR_SEGMENT_POINT;
#else
typedef uint64_t KEY_TYPE;
typedef uint64_t PLR_SEGMENT_POINT;

#endif

#endif
