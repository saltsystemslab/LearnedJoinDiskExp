#include <filesystem>
#include <stack>
#include <vector>

#include "config.h"
#include "slice.h"
#include "slice_comparator.h"
using namespace std;

static SliceComparator sc;
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

void read_sstable(std::string sstable, char *rand_nums,
                  uint64_t key_len_bytes) {
  int fd = open(sstable.c_str(), O_RDONLY);
  read(fd, rand_nums, key_len_bytes);
  close(fd);
}

void qsort(char *arr, int64_t key_len_bytes, int64_t s, int64_t e) {
  uint64_t iters = 0;
  char c1;
  char c2;
  std::stack<pair<int64_t, int64_t>> q;
  q.push(std::pair<int64_t, int64_t>(s, e));
  while (!q.empty()) {
    int64_t start = q.top().first;
    int64_t end = q.top().second;
    int64_t mid = start + (end - start) / 2; // TODO: Pick a random key.
    q.pop();
    iters++;
    if (start >= end) {
      continue;
    }
    int64_t pivot = end;
    for (int i = 0; i < key_len_bytes; i++) {
      c1 = arr[pivot * key_len_bytes + i];
      c2 = arr[mid * key_len_bytes + i];
      arr[mid * key_len_bytes + i] = c1;
      arr[pivot * key_len_bytes + i] = c2;
    }
    Slice pivot_slice(arr + pivot * key_len_bytes, key_len_bytes);
    int64_t temp_pivot = start - 1;
    int64_t idx;
    for (idx = start; idx < end; idx++) {
      Slice cur(arr + idx * key_len_bytes, key_len_bytes);
      if (sc.compare(cur, pivot_slice) <= 0) {
        temp_pivot += 1;
        if (temp_pivot == idx)
          continue;
        for (int i = 0; i < key_len_bytes; i++) {
          c1 = arr[temp_pivot * key_len_bytes + i];
          c2 = arr[idx * key_len_bytes + i];
          arr[idx * key_len_bytes + i] = c1;
          arr[temp_pivot * key_len_bytes + i] = c2;
        }
      }
    }
    temp_pivot += 1;
    for (int i = 0; i < key_len_bytes; i++) {
      c1 = arr[temp_pivot * key_len_bytes + i];
      c2 = arr[pivot * key_len_bytes + i];
      arr[pivot * key_len_bytes + i] = c1;
      arr[temp_pivot * key_len_bytes + i] = c2;
    }
    if (start < temp_pivot - 1)
      q.push(std::pair<int64_t, int64_t>(start, temp_pivot - 1));
    if (temp_pivot + 1 < end)
      q.push(std::pair<int64_t, int64_t>(temp_pivot + 1, end));
  }
}

char *generate_keys(std::string sstable_name, uint64_t num_keys,
                    int key_len_bytes) {
  uint64_t bytes_to_alloc = num_keys * key_len_bytes;
  char *rand_nums = new char[bytes_to_alloc];

  bool is_sorted = false;
  printf("Generating rand bytes!\n");
  if (std::filesystem::exists(sstable_name)) {
    read_sstable(sstable_name, rand_nums, num_keys * key_len_bytes);
    is_sorted = true;
  } else {
    fill_rand_bytes(rand_nums, bytes_to_alloc);
    is_sorted = false;
  }
  printf("Done Generating rand bytes!\n");
  /*
  for (uint64_t i = 0; i < bytes_to_alloc; i += key_len_bytes) {
    std::string key(rand_nums + i, key_len_bytes);
    keys.push_back(key);
  }
  */
  if (!is_sorted) {
    printf("Beginning sorting\n");
    qsort(rand_nums, key_len_bytes, 0, num_keys - 1);
    printf("Finished sorting\n");
    int fd = open(sstable_name.c_str(), O_WRONLY | O_CREAT, 0644);
    printf("Beginning write\n");
    pwrite(fd, rand_nums, bytes_to_alloc, 0);
    printf("Finished write\n");
    close(fd);
  }
  return rand_nums;
}
