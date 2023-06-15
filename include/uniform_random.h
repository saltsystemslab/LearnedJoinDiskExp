#include "config.h"
#include <vector>
using namespace std;

void rand_bytes(unsigned char *v, size_t n) {
  uint64_t i = 0;
  int fd = open("/dev/random", O_RDONLY);
  read(fd, v, n);
}

vector<KEY_TYPE> generate_keys(uint64_t num_keys) {
  uint64_t bytes_to_alloc = num_keys * sizeof(KEY_TYPE);
  unsigned char *rand_nums = new unsigned char[bytes_to_alloc];
  rand_bytes(rand_nums, bytes_to_alloc);

  vector<KEY_TYPE> keys;
  for (uint64_t i = 0; i < bytes_to_alloc; i += sizeof(KEY_TYPE)) {
    KEY_TYPE k = *(KEY_TYPE *)(rand_nums + i);
    keys.push_back(k);
  }
  delete rand_nums;
  printf("Done generating random bytes\n");
  sort(keys.begin(), keys.end());
  printf("Done adding to vector and sorting!\n");
  return keys;
}