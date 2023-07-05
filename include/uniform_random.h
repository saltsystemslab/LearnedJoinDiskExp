#ifndef UNIFORM_RANDOM_H
#define UNIFORM_RANDOM_H

#include <filesystem>
#include <stack>
#include <vector>
#include <thread>
#include <mutex>
#include <queue>

#include "iterator.h"
#include "config.h"
#include "slice.h"
#include "slice_comparator.h"
using namespace std;

void fill_rand_bytes(char *v, uint64_t n) {
  std::random_device rd;  // a seed source for the random number engine
  std::mt19937 gen(rd()); // mersenne_twister_engine seeded with rd()
  uint64_t u = -1;        // All 1's
  std::uniform_int_distribution<uint64_t> distrib(0, u);
  for (uint64_t i = 0; i < n; i += 8) {
    uint64_t key = distrib(gen);
    memcpy(v + i, &key, 8);
  }
}

#endif