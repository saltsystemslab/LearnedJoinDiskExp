#include <cstddef>
#include <cstdint>
namespace uniform_dense_200M_uint64_8 {
bool load(char const* dataPath);
void cleanup();
const size_t RMI_SIZE = 24592;
const uint64_t BUILD_TIME_NS = 9452939773;
const char NAME[] = "uniform_dense_200M_uint64_8";
uint64_t lookup(uint64_t key, size_t* err);
}

#include "uniform_dense_200M_uint64_8_data.h"
#include "uniform_dense_200M_uint64_8.cpp"