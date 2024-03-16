#ifndef LEARNEDINDEXMERGE_BENCHMARK_H
#define LEARNEDINDEXMERGE_BENCHMARK_H

#include "key_value_slice.h"
#include "sstable.h"
#include <stdlib.h>
#include <unistd.h>

namespace li_merge {
struct ActionResult {
  uint64_t duration_ns;
  SSTable<KVSlice> *outputTable;
};

// Output a table (to be used for joins or merges) from an SOSD dataset file.
class CreateInputTablesFromSosdDataset {
  // Keys and values from the SOSD dataset are always 8 bytes.
  const int value_size_bytes = 8;
  const int key_size_bytes = 8;
  std::string dataset_path;
  int fraction_of_keys_to_extract;
  // Where to output the input table on disk.
  std::string result_path;

  uint64_t get_num_keys(int fd) {
    // The number of keys is an unsigned 64 bit integer in the first 8 bytes
    // of the SOSD dataset.
    char bytes[8];
    int bytes_read = pread(fd, bytes, 8, 0); // Read first 8 bytes.
    if (bytes_read < 0) {
      perror("pread");
      exit(EXIT_FAILURE);
    }
    uint64_t *num_keys = (uint64_t *)bytes;
    return *num_keys;
  }
  void fix_value(char *buf, int key_len, int value_len) {
    char *key_buf = buf;
    char *value_buf = buf + key_len;
    // We copy the key as value. This is so diffs match on identitcal keys when
    // there are multiple orders.
    memset(value_buf, 0, value_len);
    memcpy(value_buf, key_buf, std::min(key_len, value_len));
  }
  std::set<uint64_t> select_indexes_uniform(uint64_t num_keys_to_select,
                                            uint64_t num_keys_in_dataset) {
    std::set<uint64_t> selected_keys;
    std::random_device rd;  // a seed source for the random number engine
    std::mt19937 gen(rd()); // mersenne_twister_engine seeded with rd()
    std::uniform_int_distribution<uint64_t> distrib(0, num_keys_in_dataset - 1);
    for (uint64_t i = 0; i < num_keys_to_select; i++) {
      uint64_t key = distrib(gen);
      selected_keys.insert(key);
    }
    fprintf(stderr, "Done Selecting Keys!");
    return selected_keys;
  }

public:
  CreateInputTablesFromSosdDataset(std::string dataset_path,
                                   std::string result_path,
                                   int fraction_of_keys_to_extract) {
    this->dataset_path = std::string(dataset_path);
    this->result_path = std::string(result_path);
    this->fraction_of_keys_to_extract = fraction_of_keys_to_extract;
  }

  ActionResult doAction() {
    int fd = open(this->dataset_path.c_str(), O_RDONLY);
    if (fd == -1) {
      perror("open");
      exit(EXIT_FAILURE);
    }

    uint64_t num_keys_in_dataset = get_num_keys(fd);
    const int bytes_to_skip_for_header = 8;
    const int inmem_cache_size_in_pages = 1;
    SSTableBuilder<KVSlice> *builder = new FixedSizeKVDiskSSTableBuilder(
        result_path, key_size_bytes, value_size_bytes);
    FixedKSizeKVFileCache dataset(
        fd, key_size_bytes, 0 /*there are *no values in sosd dataset*/,
        bytes_to_skip_for_header, inmem_cache_size_in_pages);

    // We're setup now.
    // Our job is to now 1) remove duplicates. 2) extract the
    // required number of keys. 3) Fix values for each key.
    // 4) Remove extreme values such as all 111111111

    // All 1's is a key PGM does not handle well, causing incorrect outputs.
    char kv_buf[key_size_bytes + value_size_bytes];
    char max_uint64[key_size_bytes + value_size_bytes];
    memset(max_uint64, -1, key_size_bytes + value_size_bytes);
    char prev_buf[key_size_bytes + value_size_bytes];
    memset(prev_buf, -1, key_size_bytes + value_size_bytes);

    if (fraction_of_keys_to_extract == 1) {
      // Extract all keys.
      for (uint64_t i = 0; i < num_keys_in_dataset; i++) {
        KVSlice k = dataset.get_kv(i);
        memcpy(kv_buf, k.data(), key_size_bytes);
        fix_value(kv_buf, key_size_bytes, value_size_bytes);
        // Remove max_uint64.
        if (memcmp(kv_buf, max_uint64, key_size_bytes + value_size_bytes) ==
            0) {
          break;
        }
        // Remove duplicate keys.
        if (memcmp(kv_buf, prev_buf, key_size_bytes + value_size_bytes) == 0) {
          continue;
        }
        builder->add(KVSlice(kv_buf, key_size_bytes, value_size_bytes));
      }
    } else {
      uint64_t num_keys_to_extract =
          num_keys_in_dataset / fraction_of_keys_to_extract;
      std::set<uint64_t> selected_indexes =
          select_indexes_uniform(num_keys_to_extract, num_keys_in_dataset);
      for (auto key_idx : selected_indexes) {
        KVSlice k = dataset.get_kv(key_idx);
        fix_value(kv_buf, key_size_bytes, value_size_bytes);
        // Remove max_uint64.
        if (memcmp(kv_buf, max_uint64, key_size_bytes + value_size_bytes) ==
            0) {
          break;
        }
        // Remove duplicate keys.
        if (memcmp(kv_buf, prev_buf, key_size_bytes + value_size_bytes) == 0) {
          continue;
        }
        builder->add(KVSlice(kv_buf, key_size_bytes, value_size_bytes));
      }
    }
    SSTable<KVSlice> *table = builder->build();
    close(fd);
    return {0, table};
  }
};

class CreateIndexesAction {
  std::string index_prefix;
  SSTable<KVSlice> *table;
  KeyToPointConverter<KVSlice> *converter;

public:
  CreateIndexesAction(std::string index_prefix, SSTable<KVSlice> *table,
                      KeyToPointConverter<KVSlice> *converter) {
    this->index_prefix = std::string(index_prefix);
    this->table = table;
    this->converter = converter;
  }
  void doAction() {
  }
};

}; // namespace li_merge

#endif