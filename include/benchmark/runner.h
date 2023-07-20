#ifndef LEARNEDMERGE_INDEX_TEST_RUNNER_H
#define LEARNEDMERGE_INDEX_TEST_RUNNER_H

#include "comparator.h"
#include "dataset.h"
#include "disk_sstable.h"
#include "greedy_plr_index.h"
#include "key_value_slice.h"
#include "merge.h"
#include "one_level_pgm_index.h"
#include "pgm_index.h"
#include "rbtree_index.h"
#include "sstable.h"
#include "synthetic.h"
#include "join.h"
#include <nlohmann/json.hpp>
#include <unordered_set>

using json = nlohmann::json;

namespace li_merge {

json run_test(json test_spec);
json run_standard_merge(json test_spec);
json run_learned_merge(json test_spec);
json run_sort_join(json test_spec);
json run_hash_join(json test_spec);
json run_inlj(json test_spec);
json create_input_sstable(json test_spec);
SSTableBuilder<KVSlice> *get_result_builder(json test_spec);
Comparator<KVSlice> *get_comparator(json test_spec);
IndexBuilder<KVSlice> *get_index_builder(json test_spec);
KeyToPointConverter<KVSlice> *get_converter(json test_spec);
SSTable<KVSlice> *load_sstable(std::string sstable_path, bool load_in_mem);

json run_test(json test_spec) {
  if (test_spec["algo"] == "standard_merge") {
    return run_standard_merge(test_spec);
  } else if (test_spec["algo"] == "learned_merge") {
    return run_learned_merge(test_spec);
  } else if (test_spec["algo"] == "create_input") {
    return create_input_sstable(test_spec);
  } else if (test_spec["algo"] == "sort_join") {
    return run_sort_join(test_spec);
  } else if (test_spec["algo"] == "hash_join") {
    return run_hash_join(test_spec);
  } else if (test_spec["algo"] == "inlj") {
    return run_inlj(test_spec);
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
    SSTable<KVSlice> *keys = generate_uniform_random_distribution(num_keys, key_size_bytes,
                                         value_size_bytes, comparator,
                                         result_table_builder);
    if (test_spec.contains("common_keys_file")) {
      SSTable<KVSlice> *common_keys = load_sstable(test_spec["common_keys_file"], true);
      // TODO(chesetti): This doesn't have to be inmem.
      SSTableBuilder<KVSlice> *in_mem_result = new FixedSizeKVInMemSSTableBuilder(
          0, key_size_bytes, value_size_bytes, get_comparator(test_spec));
      json merge_log;
      SSTable<KVSlice> *merged_key_list = standardMerge(common_keys, keys, comparator, in_mem_result, &merge_log);

      delete result_table_builder;
      auto iterator = merged_key_list->iterator();
      iterator->seekToFirst();
      result_table_builder = get_result_builder(test_spec);
      while (iterator->valid()) {
        result_table_builder->add(iterator->key());
        iterator->next();
      }
      result_table_builder->build();
    }
  }
  else if (test_spec["method"] == "select_common_keys") {
    std::string source = test_spec["source"];
    uint64_t num_keys_to_select = test_spec["num_keys"];
    int fd = open(source.c_str(), O_RDONLY);
    uint64_t num_keys_in_dataset = get_num_keys_from_sosd_dataset(fd);
    generate_uniform_random_indexes(num_keys_to_select, num_keys_in_dataset, result_table_builder);
    close(fd);
  } else if (test_spec["method"] == "from_sosd") {
    std::string source = test_spec["source"];
    int fd = open(source.c_str(), O_RDONLY);
    uint64_t num_keys_in_dataset = get_num_keys_from_sosd_dataset(fd);
    std::set<uint64_t> common_keys;
    if (test_spec.contains("common_keys_file")) {
      load_common_key_indexes(test_spec["common_keys_file"], &common_keys);
    }
    if (test_spec.contains("use_all") && test_spec["use_all"] == true) {
      num_keys = num_keys_in_dataset;
    }
    generate_from_datafile(fd, 8, key_size_bytes, value_size_bytes,
                           num_keys_in_dataset, num_keys, common_keys,
                           get_result_builder(test_spec));
    close(fd);
  } else if (test_spec["method"] == "from_ar") {
    // TODO(chesetti): Follow the same header format as sosd.
    std::string source = test_spec["source"];
    int fd = open(source.c_str(), O_RDONLY);
    uint64_t num_keys_in_dataset = get_num_keys_from_ar(fd);
    std::set<uint64_t> common_keys;
    if (test_spec.contains("common_keys_file")) {
      load_common_key_indexes(test_spec["common_keys_file"], &common_keys);
    }
    if (test_spec.contains("use_all") && test_spec["use_all"] == true) {
      num_keys = num_keys_in_dataset;
    }
    generate_from_datafile(fd, 16, key_size_bytes, value_size_bytes,
                           num_keys_in_dataset, num_keys, common_keys,
                           get_result_builder(test_spec));
    close(fd);
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

  json merge_log;
  auto merge_start = std::chrono::high_resolution_clock::now();
  standardMerge<KVSlice>(inner_table, outer_table, comparator,
                         result_table_builder, &merge_log);
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
  result["merge_log"] = merge_log;
  return result;
}

json run_sort_join(json test_spec) {
  json result;
  SSTable<KVSlice> *inner_table =
      load_sstable(test_spec["inner_table"], test_spec["load_sstable_in_mem"]);
  SSTable<KVSlice> *outer_table =
      load_sstable(test_spec["outer_table"], test_spec["load_sstable_in_mem"]);
  SSTableBuilder<KVSlice> *result_table_builder = get_result_builder(test_spec);
  Comparator<KVSlice> *comparator = get_comparator(test_spec);

  auto merge_start = std::chrono::high_resolution_clock::now();
  presorted_merge_join<KVSlice>(inner_table, outer_table, comparator,
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

json run_hash_join(json test_spec) {
  json result;
  SSTable<KVSlice> *inner_table =
      load_sstable(test_spec["inner_table"], test_spec["load_sstable_in_mem"]);
  SSTable<KVSlice> *outer_table =
      load_sstable(test_spec["outer_table"], test_spec["load_sstable_in_mem"]);
  SSTableBuilder<KVSlice> *result_table_builder = get_result_builder(test_spec);

  std::unordered_set<std::string> outer_index;
  Iterator<KVSlice> *outer_iter= outer_table->iterator();
  while (outer_iter->valid()) {
    KVSlice kv = outer_iter->key();
    outer_index.insert(std::string(kv.data(), kv.key_size_bytes()));
    outer_iter->next();
  }

  auto merge_start = std::chrono::high_resolution_clock::now();
  hash_join<KVSlice>(outer_index, inner_table, result_table_builder);
  auto merge_end = std::chrono::high_resolution_clock::now();
  auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                         merge_end - merge_start)
                         .count();
  float duration_sec = duration_ns / 1e9;

  delete inner_table;
  delete outer_table;
  delete result_table_builder;

  result["outer_index_size"] = outer_index.size() * (uint64_t)test_spec["key_size"];
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
  IndexBuilder<KVSlice> *inner_index_builder = get_index_builder(test_spec);
  IndexBuilder<KVSlice> *outer_index_builder = get_index_builder(test_spec);
  Index<KVSlice> *outer_index = build_index(outer_table, outer_index_builder);
  Index<KVSlice> *inner_index = build_index(inner_table, inner_index_builder);
  Comparator<KVSlice> *comparator = get_comparator(test_spec);
  SSTableBuilder<KVSlice> *result_table_builder = get_result_builder(test_spec);

  json merge_log;
  auto merge_start = std::chrono::high_resolution_clock::now();
  mergeWithIndexes(inner_table, outer_table, inner_index, outer_index,
                   comparator, result_table_builder, &merge_log);
  auto merge_end = std::chrono::high_resolution_clock::now();
  auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                         merge_end - merge_start)
                         .count();
  float duration_sec = duration_ns / 1e9;

  result["duration_ns"] = duration_ns;
  result["duration_sec"] = duration_sec;
  result["merge_log"] = merge_log;
  result["inner_index_size"] = inner_index->size_in_bytes();
  result["outer_index_size"] = outer_index->size_in_bytes();

  delete inner_table;
  delete outer_table;
  delete inner_index_builder;
  delete outer_index_builder;
  delete result_table_builder;
  delete inner_index;
  delete outer_index;
  delete comparator;

  return result;
}

json run_inlj(json test_spec) {
  json result;
  SSTable<KVSlice> *inner_table =
      load_sstable(test_spec["inner_table"], test_spec["load_sstable_in_mem"]);
  SSTable<KVSlice> *outer_table =
      load_sstable(test_spec["outer_table"], test_spec["load_sstable_in_mem"]);
  IndexBuilder<KVSlice> *inner_index_builder = get_index_builder(test_spec);
  IndexBuilder<KVSlice> *outer_index_builder = get_index_builder(test_spec);
  Index<KVSlice> *outer_index = build_index(outer_table, outer_index_builder);
  Index<KVSlice> *inner_index = build_index(inner_table, inner_index_builder);
  Comparator<KVSlice> *comparator = get_comparator(test_spec);
  SSTableBuilder<KVSlice> *result_table_builder = get_result_builder(test_spec);

  json merge_log;
  auto merge_start = std::chrono::high_resolution_clock::now();
  indexed_nested_loop_join<KVSlice>(outer_table, inner_table, inner_index,
                   comparator, result_table_builder);
  auto merge_end = std::chrono::high_resolution_clock::now();
  auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                         merge_end - merge_start)
                         .count();
  float duration_sec = duration_ns / 1e9;

  result["duration_ns"] = duration_ns;
  result["duration_sec"] = duration_sec;
  result["merge_log"] = merge_log;
  result["inner_index_size"] = inner_index->size_in_bytes();
  result["outer_index_size"] = outer_index->size_in_bytes();

  delete inner_table;
  delete outer_table;
  delete inner_index_builder;
  delete outer_index_builder;
  delete result_table_builder;
  delete inner_index;
  delete outer_index;
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

IndexBuilder<KVSlice> *get_index_builder(json test_spec) {
  std::string index_type = test_spec["index"]["type"];
  if (index_type == "onelevel_pgm16") {
    return new OneLevelPgmIndexBuilder<KVSlice, 16>(0,
                                                    get_converter(test_spec));
  } else if (index_type == "onelevel_pgm64") {
    return new OneLevelPgmIndexBuilder<KVSlice, 64>(0,
                                                      get_converter(test_spec));
  } else if (index_type == "onelevel_pgm256") {
    return new OneLevelPgmIndexBuilder<KVSlice, 256>(0,
                                                      get_converter(test_spec));
  } else if (index_type == "pgm16") {
    return new PgmIndexBuilder<KVSlice, 16>(0, get_converter(test_spec));
  } else if (index_type == "pgm64") {
    return new PgmIndexBuilder<KVSlice, 64>(0, get_converter(test_spec));
  } else if (index_type == "pgm256") {
    return new PgmIndexBuilder<KVSlice, 256>(0, get_converter(test_spec));
  } else if (index_type == "rbtree") {
    return new RbTreeIndexBuilder(get_comparator(test_spec),
                                  test_spec["key_size"]);
  } else if (index_type == "plr") {
    double error_bound = test_spec["index"]["error_bound"];
    return new GreedyPLRIndexBuilder<KVSlice>(error_bound,
                                              get_converter(test_spec));
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
} // namespace li_merge

#endif