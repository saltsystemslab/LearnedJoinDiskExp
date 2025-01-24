#include <cstddef>
#include <cstdint>
namespace uniform_dense_200M_uint64_0 {
bool load(char const* dataPath);
void cleanup();
const size_t RMI_SIZE = 419430400;
const uint64_t BUILD_TIME_NS = 14785153936;
const char NAME[] = "uniform_dense_200M_uint64_0";
uint64_t lookup(uint64_t key, size_t* err);
}

#include "uniform_dense_200M_uint64_0_data.h"
#include "uniform_dense_200M_uint64_0.cpp"