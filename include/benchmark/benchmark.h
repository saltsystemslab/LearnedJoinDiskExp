#ifndef LEARNEDINDEXMERGE_BENCHMARK_H
#define LEARNEDINDEXMERGE_BENCHMARK_H

#include "config.h"
#include "comparator.h"
#include "key_value_slice.h"
#include "sstable.h"
#include "radix_spline_index.h"
#include "alex_index.h"
#include <stdlib.h>
#include <unistd.h>

#include <nlohmann/json.hpp>
using json = nlohmann::json;

namespace li_merge {
struct CreateInputTableResult {
  uint64_t duration_ns;
  SSTable<KVSlice> *outputTable;
};


class CreateInputTable {
public:
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

  virtual SSTable<KVSlice> *createInputTable() = 0;
  CreateInputTableResult profileAndCreateInputTable() {
    auto table_create_start = std::chrono::high_resolution_clock::now();
    SSTable<KVSlice> *table = createInputTable();
    auto table_create_end = std::chrono::high_resolution_clock::now();
    auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                           table_create_end - table_create_start)
                           .count();
    return {duration_ns, table};
  };
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
};

class Create16ByteStringTables : public CreateInputTable {
  const int value_size_bytes = 16;
  const int key_size_bytes = 16;
  std::string source_path;
  std::string result_path;
  uint64_t num_keys;
  uint64_t fraction_of_keys;
  bool from_source;

  char *create_uniform_random_distribution_buffer() {
    Comparator<KVSlice> *comparator = new KVSliceMemcmp();
    uint64_t buf_size = num_keys * (key_size_bytes + value_size_bytes);
    char *data = new char[buf_size];
    for (uint64_t i = 0; i < buf_size; i += 4096) {
      RAND_bytes((unsigned char *)data + i, 4096);
    }
    char *ptr = data;
    for (uint64_t i = 0; i < num_keys; i++) {
      fix_value(ptr, key_size_bytes, value_size_bytes);
      ptr += (key_size_bytes + value_size_bytes);
    }
    delete comparator;
    return data;
  }
  char **sort_buffer(char *data) {
    char **data_ptrs = new char *[num_keys];
    Comparator<KVSlice> *comparator = new KVSliceMemcmp();
    for (uint64_t i = 0; i < num_keys; i++) {
      data_ptrs[i] = data + (i * (key_size_bytes + value_size_bytes));
    }
    std::sort(std::execution::par_unseq, data_ptrs, data_ptrs + num_keys,
              [comparator](const char *a, const char *b) -> bool {
                KVSlice kv1(a, 16, 0);
                KVSlice kv2(b, 16, 0);
                return comparator->compare(kv1, kv2) <= 0;
              });
    return data_ptrs;
  }

public:
  Create16ByteStringTables(std::string result_path, uint64_t num_keys) {
    this->result_path = std::string(result_path);
    this->num_keys = num_keys;
    from_source = false;
  }

  Create16ByteStringTables(std::string result_path, std::string source_path,
                           uint64_t fraction_of_keys) {
    this->source_path = std::string(source_path);
    this->result_path = std::string(result_path);
    this->fraction_of_keys = fraction_of_keys;
    from_source = true;
  }
  SSTable<KVSlice> *createInputTable() override {
    SSTableBuilder<KVSlice> *builder = new FixedSizeKVDiskSSTableBuilder(
        result_path, key_size_bytes, value_size_bytes);
    if (from_source) {
      SSTable<KVSlice> *table = new FixedSizeKVDiskSSTable(source_path);
      auto iter = table->iterator();
      uint64_t num_keys_in_dataset = iter->numElts();
      uint64_t num_keys_to_extract = num_keys_in_dataset / fraction_of_keys;
      std::set<uint64_t> selected_indexes =
          select_indexes_uniform(num_keys_to_extract, num_keys_in_dataset);
      char kv_buf[key_size_bytes + value_size_bytes];
      char max_uint64[key_size_bytes + value_size_bytes];
      memset(max_uint64, -1, key_size_bytes + value_size_bytes);
      char prev_buf[key_size_bytes + value_size_bytes];
      memset(prev_buf, -1, key_size_bytes + value_size_bytes);
      for (auto key_idx : selected_indexes) {
        iter->seekTo(key_idx);
        KVSlice k = iter->key();
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
        memcpy(prev_buf, kv_buf, key_size_bytes + value_size_bytes);
        builder->add(KVSlice(kv_buf, key_size_bytes, value_size_bytes));
      }
    } else {
      char *data = create_uniform_random_distribution_buffer();
      char **data_ptrs = sort_buffer(data);
      for (uint64_t i = 0; i < num_keys; i++) {
        KVSlice kv(data_ptrs[i], key_size_bytes, value_size_bytes);
        builder->add(kv);
      }
      delete[] data_ptrs;
      delete[] data;
    }
    return builder->build();
  }
};

// Output a table (to be used for joins or merges) from an SOSD dataset file.
class CreateInputTablesFromSosdDataset : public CreateInputTable {
  // Keys and values from the SOSD dataset are always 8 bytes.
  const int value_size_bytes = 8;
  const int key_size_bytes = 8;
  std::string dataset_path;
  int fraction_of_keys_to_extract;
  // Where to output the input table on disk.
  std::string result_path;

  void fix_value(char *buf, int key_len, int value_len) {
    char *key_buf = buf;
    char *value_buf = buf + key_len;
    // We copy the key as value. This is so diffs match on identitcal keys
    // when there are multiple orders.
    memset(value_buf, 0, value_len);
    memcpy(value_buf, key_buf, std::min(key_len, value_len));
  }

public:
  CreateInputTablesFromSosdDataset(std::string dataset_path,
                                   std::string result_path,
                                   int fraction_of_keys_to_extract) {
    this->dataset_path = std::string(dataset_path);
    this->result_path = std::string(result_path);
    this->fraction_of_keys_to_extract = fraction_of_keys_to_extract;
  }

  SSTable<KVSlice> *createInputTable() override {
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
        memcpy(prev_buf, kv_buf, key_size_bytes + value_size_bytes);
        builder->add(KVSlice(kv_buf, key_size_bytes, value_size_bytes));
      }
    } else {
      uint64_t num_keys_to_extract =
          num_keys_in_dataset / fraction_of_keys_to_extract;
      std::set<uint64_t> selected_indexes =
          select_indexes_uniform(num_keys_to_extract, num_keys_in_dataset);
      for (auto key_idx : selected_indexes) {
        KVSlice k = dataset.get_kv(key_idx);
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
        memcpy(prev_buf, kv_buf, key_size_bytes + value_size_bytes);
        builder->add(KVSlice(kv_buf, key_size_bytes, value_size_bytes));
      }
    }
    SSTable<KVSlice> *table = builder->build();
    close(fd);
    return table;
  }
};

class CreateUnsortedTable : public CreateInputTable {
  const uint64_t key_size_bytes = 8;
  const uint64_t value_size_bytes = 8;
  std::string dataset_path_;
  std::string result_path_;
  uint64_t fraction_keys_to_extract_;
public:
  CreateUnsortedTable(std::string result_path, std::string dataset_path, double fraction_keys_to_extract):
  dataset_path_(dataset_path), result_path_(result_path), fraction_keys_to_extract_(fraction_keys_to_extract) {
  };
  SSTable<KVSlice> *createInputTable() override {
    int fd = open(this->dataset_path_.c_str(), O_RDONLY);
    if (fd == -1) {
      perror("open");
      exit(EXIT_FAILURE);
    }

    char kv_buf[key_size_bytes + value_size_bytes];
    uint64_t num_keys_in_dataset = get_num_keys(fd);
    const int bytes_to_skip_for_header = 8;
    const int inmem_cache_size_in_pages = 1;
    SSTableBuilder<KVSlice> *builder = new FixedSizeKVDiskSSTableBuilder(
        result_path_, key_size_bytes, value_size_bytes);
    FixedKSizeKVFileCache dataset(
        fd, key_size_bytes, 0 /*there are *no values in sosd dataset*/,
        bytes_to_skip_for_header, inmem_cache_size_in_pages);

    uint64_t num_keys_to_extract =
        num_keys_in_dataset / fraction_keys_to_extract_;
      
    std::vector<uint64_t> key_indexes;
    // -1 because to avoid handle 0xfffffffff in fb for now.
    // PGM doesn't handle this well.
    for (uint64_t i=0; i<num_keys_in_dataset-1; i++) {
      key_indexes.push_back(i);
    }
    std::random_device rd;  // a seed source for the random number engine
    std::mt19937 gen(rd()); // mersenne_twister_engine seeded with rd()
    std::shuffle(key_indexes.begin(), key_indexes.end(), gen);
    for (uint64_t i = 0; i < num_keys_to_extract; i++) {
      KVSlice k = dataset.get_kv(key_indexes[i]);
      uint64_t kval = *(uint64_t *)k.data();
      memcpy(kv_buf, k.data(), key_size_bytes);
      fix_value(kv_buf, key_size_bytes, value_size_bytes);
      builder->add(KVSlice(kv_buf, key_size_bytes, value_size_bytes));
    }
    return builder->build();
  }
};

class CreateIndexes {
  std::string index_prefix;
  SSTable<KVSlice> *table;
  KeyToPointConverter<KVSlice> *converter;

  json create_index(std::string table_name, std::string index_name,
                    Iterator<KVSlice> *iter, IndexBuilder<KVSlice> *builder) {
    auto index_load_start = std::chrono::high_resolution_clock::now();
    iter->seekToFirst();
    while (iter->valid()) {
      builder->add(iter->key());
      iter->next();
    }
    auto index_load_end = std::chrono::high_resolution_clock::now();

    auto index_build_start = std::chrono::high_resolution_clock::now();
    auto index = builder->build();
    auto index_build_end = std::chrono::high_resolution_clock::now();
    json result;
    result["index_name"] = index_name;
    result["index_size"] = index->sizeInBytes();
    result["index_build_duration"] =
        std::chrono::duration_cast<std::chrono::nanoseconds>(index_build_end -
                                                             index_build_start)
            .count();
    result["index_load_duration"] =
        std::chrono::duration_cast<std::chrono::nanoseconds>(index_load_end -
                                                             index_load_start)
            .count();
    builder->backToFile(table_name + "_" + index_name);
    return result;
  }

public:
  CreateIndexes(std::string index_prefix, SSTable<KVSlice> *table,
                KeyToPointConverter<KVSlice> *converter) {
    this->index_prefix = std::string(index_prefix);
    this->table = table;
    this->converter = converter;
  }
  json doAction() {
    std::map<std::string, IndexBuilder<KVSlice> *> indexBuilders;

#ifdef STRING_KEYS
    uint64_t key_size = 16;
#else
    uint64_t key_size = 16;
#endif

    /*
    indexBuilders["btree256"] = new BTreeIndexBuilder(256, key_size);
    indexBuilders["btree1024"] = new BTreeIndexBuilder(1024, key_size);
    indexBuilders["btree4096"] = new BTreeIndexBuilder(4096, key_size);
    indexBuilders["pgm256"] = new PgmIndexBuilder<KVSlice, 128, 1>(converter);
    indexBuilders["pgm1024"] = new PgmIndexBuilder<KVSlice, 512, 1>(converter);
    indexBuilders["pgm4096"] = new PgmIndexBuilder<KVSlice, 2048, 1>(converter);

    indexBuilders["flatpgm256"] =
        new OneLevelPgmIndexBuilder<KVSlice, 128, 1>(converter);
    indexBuilders["flatpgm1024"] =
        new OneLevelPgmIndexBuilder<KVSlice, 512, 1>(converter);
    indexBuilders["flatpgm4096"] =
        new OneLevelPgmIndexBuilder<KVSlice, 2048, 1>(converter);

    indexBuilders["sampledpgm256"] =
        new PgmIndexBuilder<KVSlice, 1, 128>(converter);
    indexBuilders["sampledpgm1024"] =
        new PgmIndexBuilder<KVSlice, 4, 128>(converter);
    indexBuilders["sampledpgm4096"] =
        new PgmIndexBuilder<KVSlice, 16, 128>(converter);
    */
   #if !USE_ALEX
    indexBuilders["sampledflatpgm256"] =
        new OneLevelPgmIndexBuilder<KVSlice, 1, 128>(converter);
    indexBuilders["sampledflatpgm1024"] =
        new OneLevelPgmIndexBuilder<KVSlice, 4, 128>(converter);
    indexBuilders["sampledflatpgm4096"] =
        new OneLevelPgmIndexBuilder<KVSlice, 16, 128>(converter);

    indexBuilders["btree256"] = new BTreeIndexBuilder(256, key_size);
    indexBuilders["pgm256"] = new PgmIndexBuilder<KVSlice, 128, 1>(converter);
    indexBuilders["flatpgm256"] =
        new OneLevelPgmIndexBuilder<KVSlice, 128, 1>(converter);
    indexBuilders["sampledpgm256"] =
        new PgmIndexBuilder<KVSlice, 1, 128>(converter);
    indexBuilders["radixspline256"] = new RadixSplineIndexBuilder<KVSlice>(converter, 256);
    indexBuilders["radixspline1024"] = new RadixSplineIndexBuilder<KVSlice>(converter, 1024);
    indexBuilders["radixspline4096"] = new RadixSplineIndexBuilder<KVSlice>(converter, 4096);
    #else
    // The name for ALEX doesn't matter, since we construct it again on the join.
    // The current AlexOnDisk implementation does not support initializing from file.
    indexBuilders["alex"] = new AlexIndexBuilder(index_prefix + "_alex"); 
    indexBuilders["radixspline256"] = new RadixSplineIndexBuilder<KVSlice>(converter, 256);
    indexBuilders["flatpgm256"] =
        new OneLevelPgmIndexBuilder<KVSlice, 128, 1>(converter);
    indexBuilders["btree256"] = new BTreeIndexBuilder(256, key_size);
    #endif

    json index_stats = json::array();
    for (auto it = indexBuilders.begin(); it != indexBuilders.end(); it++) {
      index_stats.push_back(
          create_index(index_prefix, it->first, table->iterator(), it->second));
    }
    return index_stats;
  }
};

}; // namespace li_merge

#endif
