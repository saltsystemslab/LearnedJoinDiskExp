#ifndef LEARNEDINDEXMERGE_MERGE_SOSD_DATASET_H
#define LEARNEDINDEXMERGE_MERGE_SOSD_DATASET_H

#include "disk_sstable.h"
#include "key_value_slice.h"
#include "sstable.h"
#include "file_page_cache.h"
#include <algorithm>
#include <fcntl.h>
#include <openssl/rand.h>
#include <random>
#include <set>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

namespace li_merge {

void fix_value(char *buf, int key_len, int value_len) {
  char *key_buf = buf;
  char *value_buf = buf + key_len;
  // We copy the key as value. This is so diffs match on identitcal keys when
  // there are multiple orders.
  memset(value_buf, 0, value_len);
  memcpy(value_buf, key_buf, std::min(key_len, value_len));
}

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

std::set<uint64_t> select_keys_uniform(uint64_t num_keys_to_select,
                                       uint64_t num_keys,
                                       std::set<uint64_t> common_keys) {
  fprintf(stderr, "Selecting Keys!\n");
  std::vector<uint64_t> selected_keys_vector;
  selected_keys_vector.reserve(num_keys_to_select);
  std::set<uint64_t> selected_keys;
  fprintf(stderr, "Done Reserving!\n");
  for (auto key : common_keys) {
    selected_keys.insert(key);
  }
  std::random_device rd;  // a seed source for the random number engine
  std::mt19937 gen(rd()); // mersenne_twister_engine seeded with rd()
  std::uniform_int_distribution<uint64_t> distrib(0, num_keys - 1);
  for (uint64_t i = 0; i < num_keys_to_select; i++) {
    uint64_t key;
    if (num_keys_to_select == num_keys) {
      key = i;
    } else {
      key = distrib(gen);
    }
    selected_keys_vector.push_back(key);
  }
  sort(selected_keys_vector.begin(), selected_keys_vector.end());
  for (auto selected_key : selected_keys_vector) {
    selected_keys.insert(selected_keys.end(), selected_key);
  }
  fprintf(stderr, "Done Selecting Keys!");
  return selected_keys;
}

SSTable<KVSlice> *generate_from_datafile(
    int fd, int header_size, int key_size_bytes, int value_size_bytes,
    uint64_t num_keys, uint64_t num_keys_to_extract,
    std::set<uint64_t> common_keys, SSTableBuilder<KVSlice> *builder) {
  bool use_all = (num_keys == num_keys_to_extract);
  FixedKSizeKVFileCache sosd_keys(fd, key_size_bytes, 0 /*no values*/,
                                  header_size, 1, false);
  if (!use_all) {
    std::set<uint64_t> selected_keys= select_keys_uniform(num_keys_to_extract, num_keys, common_keys);
    char kv_buf[key_size_bytes + value_size_bytes];
    char prev_buf[key_size_bytes + value_size_bytes];
    memset(prev_buf, -1, key_size_bytes + value_size_bytes);
    for (auto key_idx : selected_keys) {
      KVSlice k = sosd_keys.get_kv(key_idx);
      memcpy(kv_buf, k.data(), key_size_bytes);
      fix_value(kv_buf, key_size_bytes, value_size_bytes);
      // Remove duplicate keys.
      if (memcmp(kv_buf, prev_buf, key_size_bytes + value_size_bytes) != 0) {
        builder->add(KVSlice(kv_buf, key_size_bytes, value_size_bytes));
      }
      memcpy(prev_buf, kv_buf, key_size_bytes+value_size_bytes);
    }
  } else {
    char kv_buf[key_size_bytes + value_size_bytes];
    for (uint64_t i=0; i < num_keys; i++) {
      KVSlice k = sosd_keys.get_kv(i);
      memcpy(kv_buf, k.data(), key_size_bytes);
      fix_value(kv_buf, key_size_bytes, value_size_bytes);
      builder->add(KVSlice(kv_buf, key_size_bytes, value_size_bytes));
    }
  }
  return builder->build();
}

void load_common_key_indexes(std::string common_keys_file_path,
                             std::set<uint64_t> *common_keys) {
  int fd = open(common_keys_file_path.c_str(), O_RDONLY);
  FixedSizeKVDiskSSTable sstable(common_keys_file_path);
  Iterator<KVSlice> *it = sstable.iterator();
  while (it->valid()) {
    KVSlice s = it->key();
    uint64_t *k = (uint64_t *)(s.data());
    common_keys->insert(*k);
    it->next();
  }
}

} // namespace li_merge

#endif
