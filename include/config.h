#ifndef CONFIG_H
#define CONFIG_H

#if USE_INT_128
typedef unsigned __int128 KEY_TYPE;
#else
typedef uint64_t KEY_TYPE;
#endif

#endif
