#ifndef LEARNEDINDEXMERGE_CONFIG
#define LEARNEDINDEXMERGE_CONFIG

#ifdef STRING_KEYS
typedef double POINT_FLOAT_TYPE;
#else
typedef uint64_t POINT_FLOAT_TYPE;
#endif

#define USE_ALEX 0

#endif