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
#include "runner.h"
#include "search.h"
#include "sstable.h"
#include "synthetic.h"
#include "benchmark.h"
#include <openssl/md5.h>
#include <unordered_set>

#include <nlohmann/json.hpp>
using json = nlohmann::json;

namespace li_merge {

json run_test(json test_spec);
json run_standard_merge(json test_spec);
json run_learned_merge(json test_spec);           // LM-2W
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
Index<KVSlice> *get_index(std::string table_path, json test_spec);
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
  Index<KVSlice> *inner_index = get_index(test_spec["inner_table"], test_spec);
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
    op = new StandardMerge<KVSlice>(outer_table, inner_table, inner_index,
                                    comparator, result_table_builder,
                                    num_threads);
  } else if (test_spec["algo"] == "learned_merge") {
    op = new LearnedMerge1Way<KVSlice>(
        outer_table, inner_table, inner_index, comparator,
        get_search_strategy(test_spec), result_table_builder, num_threads);
  } else if (test_spec["algo"] == "sort_join") {
    op = new SortJoin<KVSlice>(outer_table, inner_table, inner_index,
                               comparator, result_table_builder, num_threads);
  } else if (test_spec["algo"] == "hash_join") {
    op = new HashJoin(outer_table, inner_table, inner_index, comparator,
                      result_table_builder, num_threads);
  } else if (test_spec["algo"] == "lsj") {
    op = new LearnedSortJoin<KVSlice>(
        outer_table, inner_table, inner_index, comparator,
        get_search_strategy(test_spec), result_table_builder, num_threads);
  } else if (test_spec["algo"] == "inlj") {
    op = new Inlj<KVSlice>(outer_table, inner_table, inner_index, comparator,
                           get_search_strategy(test_spec), result_table_builder,
                           num_threads);
  }
  TableOpResult<KVSlice> result = op->profileOp();
  if (test_spec.contains("check_checksum") && test_spec["check_checksum"]) {
    result.stats["checksum"] = md5_checksum(result.output_table);
  } else {
    result.stats["checksum"] = 0;
  }
  result.stats["num_output_keys"] = result.output_table->iterator()->numElts();
  return result.stats;
}

json create_index(std::string table_name, std::string index_name, Iterator<KVSlice> *iter,
                  IndexBuilder<KVSlice> *builder) {
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

json create_indexes(SSTable<KVSlice> *table,
                    KeyToPointConverter<KVSlice> *converter,
                    std::string table_name, json test_spec) {
  auto sampledpgm256 = new PgmIndexBuilder<KVSlice, 1, 128>(converter);
  auto sampledpgm1024 = new PgmIndexBuilder<KVSlice, 4, 128>(converter);
  auto sampledpgm4096 = new PgmIndexBuilder<KVSlice, 16, 128>(converter);

  auto pgm256 = new PgmIndexBuilder<KVSlice, 128, 1>(converter);
  auto pgm1024 = new PgmIndexBuilder<KVSlice, 512, 1>(converter);
  auto pgm4096 = new PgmIndexBuilder<KVSlice, 2048, 1>(converter);

  auto flatpgm256 = new OneLevelPgmIndexBuilder<KVSlice, 128, 1>(converter);
  auto flatpgm1024 = new OneLevelPgmIndexBuilder<KVSlice, 512, 1>(converter);
  auto flatpgm4096 = new OneLevelPgmIndexBuilder<KVSlice, 2048, 1>(converter);

  auto sampledflatpgm256 = 
    new OneLevelPgmIndexBuilder<KVSlice, 1, 128>(converter);
  auto sampledflatpgm1024 = 
    new OneLevelPgmIndexBuilder<KVSlice, 4, 128>(converter);
  auto sampledflatpgm4096 = 
    new OneLevelPgmIndexBuilder<KVSlice, 16, 128>(converter);

  auto btree256 = new BTreeIndexBuilder(
      1 * (uint64_t)test_spec["key_size"] / 8, test_spec["key_size"],
      test_spec["value_size"]);
  auto btree1024 = new BTreeIndexBuilder(
      4 * (uint64_t)test_spec["key_size"] / 8, test_spec["key_size"],
      test_spec["value_size"]);
  auto btree4096 = new BTreeIndexBuilder(
      16 * (uint64_t)test_spec["key_size"] / 8, test_spec["key_size"],
      test_spec["value_size"]);

  json index_stats = json::array();

  index_stats.push_back(
      create_index(table_name, "sampledpgm256", table->iterator(), sampledpgm256));
  index_stats.push_back(
      create_index(table_name, "sampledpgm1024", table->iterator(), sampledpgm1024));
  index_stats.push_back(
      create_index(table_name, "sampledpgm4096", table->iterator(), sampledpgm4096));

  index_stats.push_back(create_index(table_name, "pgm256", table->iterator(), pgm256));
  index_stats.push_back(create_index(table_name, "pgm1024", table->iterator(), pgm1024));
  index_stats.push_back(create_index(table_name, "pgm4096", table->iterator(), pgm4096));

  index_stats.push_back(
      create_index(table_name, "flatpgm256", table->iterator(), flatpgm256));
  index_stats.push_back(
      create_index(table_name, "flatpgm1024", table->iterator(), flatpgm1024));
  index_stats.push_back(
      create_index(table_name, "flatpgm4096", table->iterator(), flatpgm4096));

  index_stats.push_back(
      create_index(table_name, "sampledflatpgm256", table->iterator(), sampledflatpgm256));
  index_stats.push_back(
      create_index(table_name, "sampledflatpgm1024", table->iterator(), sampledflatpgm1024));
  index_stats.push_back(
      create_index(table_name, "sampledflatpgm4096", table->iterator(), sampledflatpgm4096));

  index_stats.push_back(create_index(table_name, "btree256", table->iterator(), btree256));
  index_stats.push_back(
      create_index(table_name, "btree1024", table->iterator(), btree1024));
  index_stats.push_back(
      create_index(table_name, "btree4096", table->iterator(), btree4096));

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
      table = generate_uniform_random_distribution(num_keys, key_size_bytes,
                                                   value_size_bytes, comparator,
                                                   result_table_builder);
    }
  } else if (test_spec["method"] == "sosd") {
    std::string source_path = test_spec["source"];
    std::string result_path = test_spec["result_path"];
    int fd = open(source_path.c_str(), O_RDONLY);
    uint64_t num_keys_in_dataset = get_num_keys_from_sosd_dataset(fd);
    std::set<uint64_t> common_keys; // UNUSED.
    CreateInputTablesFromSosdDataset action(source_path, result_path, num_keys_in_dataset/num_keys);
    table = action.doAction().outputTable;
    close(fd);
  } else {
    fprintf(stderr, "Unsupported input creation method!");
    abort();
  }
  if (test_spec["create_indexes"]) {
    result["index_stats"] =
        create_indexes(table, get_converter(test_spec), result_path, test_spec);
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

Index<KVSlice> *get_index(std::string table_path, json test_spec) {
  if (!test_spec.contains("index")) {
    return new PgmIndex<KVSlice, 256, 1>(table_path + "_pgm256",
                                      get_converter(test_spec));
  }
  std::string index_type = test_spec["index"]["type"];
  if (index_type == "sampledpgm256") {
    return new PgmIndex<KVSlice, 1, 128>(table_path + "_sampledpgm256",
                                      get_converter(test_spec));
  } else if (index_type == "sampledpgm1024") {
    return new PgmIndex<KVSlice, 4, 128>(table_path + "_sampledpgm1024",
                                      get_converter(test_spec));
  } else if (index_type == "sampledpgm4096") {
    return new PgmIndex<KVSlice, 16, 128>(table_path + "_sampledpgm4096",
                                      get_converter(test_spec));
  } else if (index_type == "pgm256") {
    return new PgmIndex<KVSlice, 128, 1>(table_path + "_pgm256",
                                      get_converter(test_spec));
  } else if (index_type == "pgm1024") {
    return new PgmIndex<KVSlice, 512, 1>(table_path + "_pgm1024",
                                      get_converter(test_spec));
  } else if (index_type == "pgm4096") {
    return new PgmIndex<KVSlice, 2048, 1>(table_path + "_pgm4096",
                                       get_converter(test_spec));
  } else if (index_type == "flatpgm256") {
    return new OneLevelPgmIndex<KVSlice, 128, 1>(table_path + "_flatpgm256",
                                              get_converter(test_spec));
  } else if (index_type == "flatpgm1024") {
    return new OneLevelPgmIndex<KVSlice, 512, 1>(table_path + "_flatpgm1024",
                                              get_converter(test_spec));
  } else if (index_type == "flatpgm4096") {
    return new OneLevelPgmIndex<KVSlice, 2048, 1>(table_path + "_flatpgm4096",
                                               get_converter(test_spec));
  } else if (index_type == "flatpgm4096") {
    return new OneLevelPgmIndex<KVSlice, 2048, 1>(table_path + "_flatpgm4096",
                                               get_converter(test_spec));
  } else if (index_type == "sampledflatpgm256") {
    return new OneLevelPgmIndex<KVSlice, 1, 128>(table_path + "_sampledflatpgm256",
                                              get_converter(test_spec));
  } else if (index_type == "sampledflatpgm1024") {
    return new OneLevelPgmIndex<KVSlice, 4, 128>(table_path + "_sampledflatpgm1024",
                                              get_converter(test_spec));
  } else if (index_type == "sampledflatpgm4096") {
    return new OneLevelPgmIndex<KVSlice, 16, 128>(table_path + "_sampledflatpgm4096",
                                               get_converter(test_spec));
  }
  
  else if (index_type == "btree") {
    uint64_t suffix = test_spec["index"]["leaf_size_in_pages"];
    suffix *= (4096 / ((uint64_t)test_spec["key_size"] +
                       (uint64_t)test_spec["value_size"]));
    return new BTreeWIndex(table_path + "_btree" + std::to_string(suffix),
                           suffix);
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
