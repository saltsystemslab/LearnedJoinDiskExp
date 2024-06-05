#include <cstddef>
#include <cstdint>
namespace fb_200M_uint64_0 {
bool load(char const* dataPath);
void cleanup();
const size_t RMI_SIZE = 402653200;
const uint64_t BUILD_TIME_NS = 18617317378;
const char NAME[] = "fb_200M_uint64_0";
uint64_t lookup(uint64_t key, size_t* err);
}

#include "fb_200M_uint64_0_data.h"
#include "fb_200M_uint64_0.cpp"