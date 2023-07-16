#ifndef LEARNEDINDEXMERGE_MERGE_SOSD_DATASET_H
#define LEARNEDINDEXMERGE_MERGE_SOSD_DATASET_H

#include "key_value_slice.h"
#include <fcntl.h>
#include <openssl/rand.h>
#include <random>
#include <set>
#include <stdlib.h>

namespace li_merge {

uint64_t get_num_keys_from_sosd_dataset(int fd) {
  char bytes[8];
  int bytes_read = pread(fd, bytes, 8, 0); // Read first 8 bytes.
  if (bytes_read < 0) {
    perror("pread");
    abort();
  }
  uint64_t *num_keys = (uint64_t *)bytes;
  return *num_keys;
}

uint64_t get_num_keys_from_ar(int fd) {
  char bytes[16];
  int bytes_read = pread(fd, bytes, 16, 0); // Read first 8 bytes.
  if (bytes_read < 0) {
    perror("pread");
    abort();
  }
  uint64_t *num_keys = (uint64_t *)bytes;
  return *num_keys;
}

std::set<uint64_t> select_keys_uniform(uint64_t num_keys_to_select,
                                       uint64_t num_keys) {
  std::random_device rd;  // a seed source for the random number engine
  std::mt19937 gen(rd()); // mersenne_twister_engine seeded with rd()
  std::uniform_int_distribution<uint64_t> distrib(0, num_keys);
  std::set<uint64_t> split_keys;
  for (uint64_t i = 0; i < num_keys_to_select; i++) {
    uint64_t key;
    if (num_keys_to_select == num_keys) {
      key = i;
    } else {
      key = distrib(gen);
    }
    split_keys.insert(key);
  }
  return split_keys;
}

SSTable<KVSlice> *generate_from_datafile(int fd, int header_size,
                                         int key_size_bytes,
                                         int value_size_bytes,
                                         uint64_t num_keys,
                                         uint64_t num_keys_to_extract,
                                         SSTableBuilder<KVSlice> *builder) {

  std::set<uint64_t> selected_keys =
      select_keys_uniform(num_keys_to_extract, num_keys);
  FixedKSizeKVFileCache sosd_keys(fd, key_size_bytes, 0 /*no values*/,
                                  header_size);
  char kv_buf[key_size_bytes + value_size_bytes];
  for (auto key_idx : selected_keys) {
    KVSlice k = sosd_keys.get_kv(key_idx);
    memcpy(kv_buf, k.data(), key_size_bytes);
    RAND_bytes((unsigned char *)(kv_buf + key_size_bytes), value_size_bytes);
    builder->add(KVSlice(kv_buf, key_size_bytes, value_size_bytes));
  }
  return builder->build();
}

} // namespace li_merge

#endif