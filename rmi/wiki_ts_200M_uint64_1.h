#include <cstddef>
#include <cstdint>
namespace wiki_ts_200M_uint64_1 {
bool load(char const* dataPath);
void cleanup();
const size_t RMI_SIZE = 201326624;
const uint64_t BUILD_TIME_NS = 18100506268;
const char NAME[] = "wiki_ts_200M_uint64_1";
uint64_t lookup(uint64_t key, size_t* err);
}

#include "wiki_ts_200M_uint64_1_data.h"
#include "wiki_ts_200M_uint64_1.cpp"
