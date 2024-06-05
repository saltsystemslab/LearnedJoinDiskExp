#include <cstddef>
#include <cstdint>
namespace osm_cellids_800M_uint64_0 {
bool load(char const* dataPath);
void cleanup();
const size_t RMI_SIZE = 402653216;
const uint64_t BUILD_TIME_NS = 56705576122;
const char NAME[] = "osm_cellids_800M_uint64_0";
uint64_t lookup(uint64_t key, size_t* err);
}

#include "osm_cellids_800M_uint64_0_data.h"
#include "osm_cellids_800M_uint64_0.cpp"