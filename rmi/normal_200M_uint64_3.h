#include <cstddef>
#include <cstdint>
namespace normal_200M_uint64_3 {
bool load(char const* dataPath);
void cleanup();
const size_t RMI_SIZE = 20512;
const uint64_t BUILD_TIME_NS = 14344723517;
const char NAME[] = "normal_200M_uint64_3";
uint64_t lookup(uint64_t key, size_t* err);
}

#include "normal_200M_uint64_3_data.h"
#include "normal_200M_uint64_3.cpp"