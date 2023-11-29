#ifndef LEARNEDMERGE_INDEX_TEST_RUNNER_H
#define LEARNEDMERGE_INDEX_TEST_RUNNER_H

#include "btree_index.h"
#include "comparator.h"
#include "dataset.h"
#include "disk_sstable.h"
#include "join.h"
#include "key_value_slice.h"
#include "merge.h"
#include "one_level_pgm_index.h"
#include "pgm_index.h"
#include "sstable.h"
#include "synthetic.h"
#include <openssl/md5.h>
#include <unordered_set>
#include "runner.h"
#include "search.h"

#include <nlohmann/json.hpp>
using json = nlohmann::json;

namespace li_merge {

json run_test(json test_spec);
json run_standard_merge(json test_spec);
json run_learned_merge(json test_spec); // LM-2W
json run_learned_merge_threshold(json test_spec); // LM-1W
json run_sort_join(json test_spec);
json run_sort_join_exp(json test_spec);
json run_hash_join(json test_spec);
json run_inlj(json test_spec);
json create_input_sstable(json test_spec);
std::string md5_checksum(SSTable<KVSlice> *sstable);
SSTableBuilder<KVSlice> *get_result_builder(json test_spec);
PSSTableBuilder<KVSlice> *get_parallel_result_builder_for_join(json test_spec);
PSSTableBuilder<KVSlice> *get_parallel_result_builder_for_merge(json test_spec);
Comparator<KVSlice> *get_comparator(json test_spec);
SearchStrategy<KVSlice> *get_search_strategy(json test_spec);
Index<KVSlice> *get_index(std::string table_path,
                                         json test_spec);
KeyToPointConverter<KVSlice> *get_converter(json test_spec);
SSTable<KVSlice> *load_sstable(std::string sstable_path, bool load_in_mem);

json run_test(json test_spec) {
  if (test_spec["algo"] == "create_input") {
    return create_input_sstable(test_spec);
  }

  SSTable<KVSlice> *inner_table =
      load_sstable(test_spec["inner_table"], test_spec["load_sstable_in_mem"]);
  SSTable<KVSlice> *outer_table =
      load_sstable(test_spec["outer_table"], test_spec["load_sstable_in_mem"]);
  Index<KVSlice> *inner_index =
      get_index(test_spec["inner_table"], test_spec);
  Comparator<KVSlice> *comparator = get_comparator(test_spec);
  int num_threads = test_spec["num_threads"];
  // TODO: Make this a function map.
  PSSTableBuilder<KVSlice> *result_table_builder; 
  if (test_spec["algo"] == "standard_merge" || 
    test_spec["algo"] == "learned_merge") {
      result_table_builder = get_parallel_result_builder_for_merge(test_spec);
  } else {
    result_table_builder = get_parallel_result_builder_for_join(test_spec);
  }

  TableOp<KVSlice> *op;
  if (test_spec["algo"] == "standard_merge") {
    op = new StandardMerge<KVSlice>(
      outer_table, inner_table, 
      inner_index, 
      comparator, 
      result_table_builder,
      num_threads);
  } else if (test_spec["algo"] == "learned_merge") {
    op = new LearnedMerge1Way<KVSlice>(
      outer_table, inner_table, 
      inner_index, 
      comparator, 
      get_search_strategy(test_spec),
      result_table_builder,
      num_threads);
  } else if (test_spec["algo"] == "sort_join") {
    op = new SortJoin<KVSlice>(
      outer_table, inner_table, 
      inner_index, 
      comparator, 
      result_table_builder,
      num_threads);
  } else if (test_spec["algo"] == "sort_join_bin") {
    op = new SortJoinBinSearch<KVSlice>(
      outer_table, inner_table, 
      inner_index, 
      comparator, 
      result_table_builder,
      num_threads);
  } else if (test_spec["algo"] == "hash_join") {
    op = new HashJoin(
      outer_table, inner_table, 
      inner_index, 
      comparator, 
      result_table_builder,
      num_threads);
  } else if (test_spec["algo"] == "inlj") {
    op = new LearnedIndexInlj<KVSlice>(
      outer_table, inner_table, 
      inner_index, 
      comparator, 
      get_search_strategy(test_spec),
      result_table_builder,
      num_threads);
  }
  TableOpResult<KVSlice> result = op->profileOp();
  if (test_spec.contains("check_checksum") && test_spec["check_checksum"]) {
    result.stats["checksum"] =  md5_checksum(result.output_table);
  } else {
    result.stats["checksum"] =  0;
  }
  return result.stats;
}

json create_index(std::string name, Iterator<KVSlice> *iter, IndexBuilder<KVSlice> *builder) {
  auto index_build_start = std::chrono::high_resolution_clock::now();
  iter->seekToFirst();
  while (iter->valid()) {
    builder->add(iter->key());
    iter->next();
  }
  auto index = builder->build();
  auto index_build_end = std::chrono::high_resolution_clock::now();
  json result;
  result["index_name"] = name;
  result["index_size"] = index->sizeInBytes();
  result["index_build_duration"] = std::chrono::duration_cast<std::chrono::nanoseconds>(
                         index_build_end - index_build_start).count();
  return result;
}

json create_indexes(SSTable<KVSlice> *table, KeyToPointConverter<KVSlice> *converter, std::string tableName, json test_spec) {
    auto pgm256 = new PgmIndexBuilder<KVSlice, 128>(0, converter, tableName + "_pgm256");
    auto pgm1024 = new PgmIndexBuilder<KVSlice, 512>(0, converter, tableName + "_pgm1024");
    auto pgm2048 = new PgmIndexBuilder<KVSlice, 1024>(0, converter, tableName + "_pgm2048");
    auto btree256 = new BTreeIndexBuilder(1 * (uint64_t)test_spec["key_size"]/8, test_spec["key_size"], test_spec["value_size"], tableName + "_btree256");
    auto btree1024 = new BTreeIndexBuilder(4 * (uint64_t)test_spec["key_size"]/8, test_spec["key_size"], test_spec["value_size"], tableName + "_btree1024");
    auto btree2048 = new BTreeIndexBuilder(8 * (uint64_t)test_spec["key_size"]/8, test_spec["key_size"], test_spec["value_size"], tableName + "_btree2048");

    json index_stats = json::array();

    index_stats.push_back(create_index("pgm256", table->iterator(), pgm256));
    index_stats.push_back(create_index("pgm1024", table->iterator(), pgm1024));
    index_stats.push_back(create_index("pgm2048", table->iterator(), pgm2048));
    index_stats.push_back(create_index("btree256", table->iterator(), btree256));
    index_stats.push_back(create_index("btree1024", table->iterator(), btree1024));
    index_stats.push_back(create_index("btree2048", table->iterator(), btree2048));

    pgm256->backToFile();
    pgm1024->backToFile();
    pgm2048->backToFile();
    btree256->backToFile();
    btree1024->backToFile();
    btree2048->backToFile();

    return index_stats;
}



json create_input_sstable(json test_spec) {
  json result;
  std::string result_path = test_spec["result_path"];
  int key_size_bytes = test_spec["key_size"];
  int value_size_bytes = test_spec["value_size"];
  uint64_t num_keys = test_spec["num_keys"];
  Comparator<KVSlice> *comparator = get_comparator(test_spec);
  SSTableBuilder<KVSlice> *result_table_builder = get_result_builder(test_spec);
  auto merge_start = std::chrono::high_resolution_clock::now();

  SSTable<KVSlice> *table;
  if (test_spec["method"] == "string") {
    // Inner table creates index, so we use it as a source.
    // This it keep 100% selectivity.
    if (test_spec["create_indexes"] == true) {
    table = generate_uniform_random_distribution(
        num_keys, key_size_bytes, value_size_bytes, comparator,
        result_table_builder);
    } else {
      std::string source = test_spec["source"];
      SSTable<KVSlice> *table = load_sstable(source, false);
      auto iter = table->iterator()->numElts();
      int fd = open(source.c_str(), O_RDONLY);
      // First 8 bytes are count, this is same for our SSTable and SOSD.
      uint64_t num_keys_in_dataset = get_num_keys_from_sosd_dataset(fd);
      std::set<uint64_t> common_keys; // UNUSED.
      table = generate_from_datafile(fd, 16 /* We use a 16 byte header*/, key_size_bytes, value_size_bytes,
                           num_keys_in_dataset, num_keys, common_keys,
                           get_result_builder(test_spec));
    }
  } else if (test_spec["method"] == "sosd") {
    std::string source = test_spec["source"];
    int fd = open(source.c_str(), O_RDONLY);
    uint64_t num_keys_in_dataset = get_num_keys_from_sosd_dataset(fd);
    std::set<uint64_t> common_keys; // UNUSED.
    table = generate_from_datafile(fd, 8, key_size_bytes, value_size_bytes,
                           num_keys_in_dataset, num_keys, common_keys,
                           get_result_builder(test_spec));
    // Create indexes for the input tables.
    close(fd);
  } else {
    fprintf(stderr, "Unsupported input creation method!");
    abort();
  }
  if (test_spec["create_indexes"]) {
    result["index_stats"] = create_indexes(table, get_converter(test_spec), result_path, test_spec);
  }
  auto merge_end = std::chrono::high_resolution_clock::now();
  auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                         merge_end - merge_start)
                         .count();
  float duration_sec = duration_ns / 1e9;
  result["input_created"] = true;
  result["duration_sec"] = duration_sec;
  delete comparator;
  return result;
}

SSTable<KVSlice> *load_sstable(std::string path, bool load_in_mem) {
  FixedSizeKVDiskSSTable *table_disk = new FixedSizeKVDiskSSTable(path);
  if (load_in_mem) {
    SSTable<KVSlice> *table_in_mem = table_disk->load_sstable_in_mem();
    delete table_disk;
    return table_in_mem;
  }
  return table_disk;
}

Comparator<KVSlice> *get_comparator(json test_spec) {
  std::string key_type = test_spec["key_type"];
  if (key_type == "uint64") {
    return new KVUint64Cmp();
  } else if (key_type == "str") {
    return new KVSliceMemcmp();
  }
  fprintf(stderr, "Unknown Key Type in test spec");
  abort();
}

SearchStrategy<KVSlice> *get_search_strategy(json test_spec) {
  std::string search = test_spec["index"]["search"];
  if (search == "linear") {
    return new LinearSearch();
  } else if (search == "binary") {
    return new BinarySearch();
  } else if (search == "exponential") {
    return new ExponentialSearch();
  }
  fprintf(stderr, "Unknown Key Type in test spec");
  abort();
}

Index<KVSlice> *get_index(std::string table_path,
                                         json test_spec) {
  if (!test_spec.contains("index")) {
    return new PgmIndex<KVSlice, 256>(table_path + "_pgm256", get_converter(test_spec));
  }
  std::string index_type = test_spec["index"]["type"];
  if (index_type == "pgm256") {
    return new PgmIndex<KVSlice, 128>(table_path + "_pgm256", get_converter(test_spec));
  } else if (index_type == "pgm1024") {
    return new PgmIndex<KVSlice, 512>(table_path + "_pgm1024", get_converter(test_spec));
  } else if (index_type == "pgm2048") {
    return new PgmIndex<KVSlice, 1024>(table_path + "_pgm2048", get_converter(test_spec));
  } else if (index_type == "pgm4096") {
    return new PgmIndex<KVSlice, 2048>(table_path + "_pgm4096", get_converter(test_spec));
  } else if (index_type == "btree") {
    uint64_t suffix = test_spec["index"]["leaf_size_in_pages"];
    suffix *= (4096 / ((uint64_t)test_spec["key_size"] + (uint64_t) test_spec["value_size"]));
    return new BTreeWIndex(table_path + "_btree" + std::to_string(suffix), suffix);
  }
  fprintf(stderr, "Unknown Index Type in test spec");
  abort();
}

KeyToPointConverter<KVSlice> *get_converter(json test_spec) {
  if (test_spec["key_type"] == "uint64") {
    return new KVUint64ToDouble();
  }
  if (test_spec["key_type"] == "str") {
    return new KVStringToDouble();
  }
  fprintf(stderr, "Unknown Key Type in test spec");
  abort();
}

SSTableBuilder<KVSlice> *get_result_builder(json test_spec) {
  bool write_result_to_disk = test_spec["write_result_to_disk"];
  int key_size_bytes = test_spec["key_size"];
  int value_size_bytes = test_spec["value_size"];
  if (write_result_to_disk) {
    std::string result_file = test_spec["result_path"];
    return new FixedSizeKVDiskSSTableBuilder(result_file, key_size_bytes,
                                             value_size_bytes);
  } else {
    return new FixedSizeKVInMemSSTableBuilder(
        0, key_size_bytes, value_size_bytes, get_comparator(test_spec));
  }
}

PSSTableBuilder<KVSlice> *
get_parallel_result_builder_for_merge(json test_spec) {
  bool write_result_to_disk = test_spec["write_result_to_disk"];
  int key_size_bytes = test_spec["key_size"];
  int value_size_bytes = test_spec["value_size"];
  if (write_result_to_disk) {
    std::string result_file = test_spec["result_path"];
    return new PFixedSizeKVDiskSSTableBuilder(result_file, key_size_bytes,
                                              value_size_bytes);
  } else {
    return new PFixedSizeKVInMemSSTableBuilder(
        0, key_size_bytes, value_size_bytes, get_comparator(test_spec));
  }
}

PSSTableBuilder<KVSlice> *get_parallel_result_builder_for_join(json test_spec) {
  bool write_result_to_disk = test_spec["write_result_to_disk"];
  int key_size_bytes = test_spec["key_size"];
  int value_size_bytes = test_spec["value_size"];
  if (write_result_to_disk) {
    std::string result_file = test_spec["result_path"];
    return new PSplitFixedSizeKVDiskSSTableBuilder(result_file, key_size_bytes,
                                                   value_size_bytes);
  } else {
    abort();
  }
}

std::string md5_checksum(SSTable<KVSlice> *sstable) {
  unsigned char *checksum = new unsigned char[MD5_DIGEST_LENGTH];
  char *md5checksum = new char[MD5_DIGEST_LENGTH * 2 + 1];
  memset(md5checksum, 0, 2 * MD5_DIGEST_LENGTH + 1);

  MD5_CTX md5_ctx;
  MD5_Init(&md5_ctx);

  Iterator<KVSlice> *iter = sstable->iterator();
  iter->seekToFirst();
  while (iter->valid()) {
    KVSlice kv = iter->key();
    MD5_Update(&md5_ctx, kv.data(), kv.total_size_bytes());
    iter->next();
  }
  MD5_Final(checksum, &md5_ctx);
  for (int i = 0; i < MD5_DIGEST_LENGTH; i++) {
    snprintf(md5checksum + 2 * i, 3, "%02X", checksum[i]);
  }
  std::string result(md5checksum);
  delete md5checksum;
  delete checksum;
  return result;
}


} // namespace li_merge

#endif
