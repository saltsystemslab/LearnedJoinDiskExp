#ifndef LEARNEDMERGE_INDEX_TEST_RUNNER_H
#define LEARNEDMERGE_INDEX_TEST_RUNNER_H

#include "comparator.h"
#include "disk_sstable.h"
#include "key_value_slice.h"
#include "merge.h"
#include "pgm_index.h"
#include "sstable.h"
#include "synthetic.h"
#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace li_merge {

json run_test(json test_spec);
json run_standard_merge(json test_spec);
json run_learned_merge(json test_spec);
json create_input_sstable(json test_spec);
SSTableBuilder<KVSlice> *get_result_builder(json test_spec);
Comparator<KVSlice> *get_comparator(json test_spec);
KeyToPointConverter<KVSlice> *get_converter(json test_spec);
SSTable<KVSlice> *load_sstable(std::string sstable_path, bool load_in_mem);

json run_test(json test_spec) {
  if (test_spec["algo"] == "standard_merge") {
    return run_standard_merge(test_spec);
  } else if (test_spec["algo"] == "learned_merge") {
    return run_learned_merge(test_spec);
  } else if (test_spec["algo"] == "create_input") {
    return create_input_sstable(test_spec);
  }
  fprintf(stderr, "Unknown algorithm in testspec!");
  abort();
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
  if (test_spec["method"] == "uniform_dist") {
    generate_uniform_random_distribution(num_keys, key_size_bytes, value_size_bytes, comparator, result_table_builder);
  } else {
    fprintf(stderr, "Unsupported input creation method!");
    abort();
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

json run_standard_merge(json test_spec) {
  json result;
  SSTable<KVSlice> *inner_table =
      load_sstable(test_spec["inner_table"], test_spec["load_sstable_in_mem"]);
  SSTable<KVSlice> *outer_table =
      load_sstable(test_spec["outer_table"], test_spec["load_sstable_in_mem"]);
  SSTableBuilder<KVSlice> *result_table_builder = get_result_builder(test_spec);
  Comparator<KVSlice> *comparator = get_comparator(test_spec);

  auto merge_start = std::chrono::high_resolution_clock::now();
  standardMerge<KVSlice>(inner_table, outer_table, comparator,
                         result_table_builder);
  auto merge_end = std::chrono::high_resolution_clock::now();
  auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
      merge_end - merge_start)
    .count();
  float duration_sec = duration_ns / 1e9;


  delete inner_table;
  delete outer_table;
  delete result_table_builder;
  delete comparator;

  result["duration_ns"] = duration_ns;
  result["duration_sec"] = duration_sec;
  return result;
}

json run_learned_merge(json test_spec) {
  json result;
  SSTable<KVSlice> *inner_table =
      load_sstable(test_spec["inner_table"], test_spec["load_sstable_in_mem"]);
  SSTable<KVSlice> *outer_table =
      load_sstable(test_spec["outer_table"], test_spec["load_sstable_in_mem"]);
  PgmIndexBuilder<KVSlice> *inner_index_builder =
      new PgmIndexBuilder(0, get_converter(test_spec));
  PgmIndexBuilder<KVSlice> *outer_index_builder =
      new PgmIndexBuilder(0, get_converter(test_spec));
  Index<KVSlice> *outer_index = build_index(outer_table, outer_index_builder);
  Index<KVSlice> *inner_index = build_index(inner_table, inner_index_builder);
  Comparator<KVSlice> *comparator = get_comparator(test_spec);
  SSTableBuilder<KVSlice> *result_table_builder = get_result_builder(test_spec);

  auto merge_start = std::chrono::high_resolution_clock::now();
  mergeWithIndexes(inner_table, outer_table, inner_index, outer_index,
                   comparator, result_table_builder);
  auto merge_end = std::chrono::high_resolution_clock::now();
  auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
      merge_end - merge_start)
    .count();
  float duration_sec = duration_ns / 1e9;

  delete inner_table;
  delete outer_table;
  delete inner_index_builder;
  delete outer_index_builder;
  delete result_table_builder;
  delete inner_index;
  delete outer_index;
  delete comparator;
  result["duration_ns"] = duration_ns;
  result["duration_sec"] = duration_sec;

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
} // namespace li_merge

#endif