#include <vector>

#include "config.h"
#include "slice_comparator.h"
using namespace std;

static SliceComparator sc;
void rand_bytes(char *v, size_t n) {
  uint64_t i = 0;
  int fd = open("/dev/random", O_RDONLY);
  read(fd, v, n);
}

vector<std::string> generate_keys(uint64_t num_keys, int key_len_bytes) {
  uint64_t bytes_to_alloc = num_keys * key_len_bytes;
  char *rand_nums = new char[bytes_to_alloc];
  rand_bytes(rand_nums, bytes_to_alloc);

  vector<std::string> keys;
  for (uint64_t i = 0; i < bytes_to_alloc; i += key_len_bytes) {
    std::string key(rand_nums + i, key_len_bytes);
    keys.push_back(key);
  }
  delete rand_nums;
  printf("Done generating random bytes\n");
  sort(keys.begin(), keys.end());
  printf("Done adding to vector and sorting!\n");
  return keys;
}
