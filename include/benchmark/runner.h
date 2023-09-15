#ifndef LEARNEDMERGE_INDEX_TEST_RUNNER_H
#define LEARNEDMERGE_INDEX_TEST_RUNNER_H

#include "comparator.h"
#include "dataset.h"
#include "disk_sstable.h"
#include "greedy_plr_index.h"
#include "join.h"
#include "key_value_slice.h"
#include "merge.h"
#include "one_level_pgm_index.h"
#include "pgm_index.h"
#include "betree_index.h"
#include "rbtree_index.h"
#include "sstable.h"
#include "synthetic.h"
#include <openssl/md5.h>
#include <unordered_set>
#include "one_level_pgm_index.h"
#include "pgm_index.h"

#include <nlohmann/json.hpp>
using json = nlohmann::json;

namespace li_merge {

json run_test(json test_spec);
json run_standard_merge(json test_spec);
json run_learned_merge(json test_spec);
json run_learned_merge_threshold(json test_spec);
json run_sort_join(json test_spec);
json run_hash_join(json test_spec);
json run_inlj(json test_spec);
json run_index_study(json test_spec);
json run_inlj_with_btree(json test_spec);
json run_inlj_with_pgm(json test_spec);
json create_input_sstable(json test_spec);
std::string md5_checksum(SSTable<KVSlice> *sstable);
SSTableBuilder<KVSlice> *get_result_builder(json test_spec);
PSSTableBuilder<KVSlice> *get_parallel_result_builder_for_join(json test_spec);
PSSTableBuilder<KVSlice> *get_parallel_result_builder_for_merge(json test_spec);
Comparator<KVSlice> *get_comparator(json test_spec);
IndexBuilder<KVSlice> *get_index_builder(std::string table_path,
                                         json test_spec);
KeyToPointConverter<KVSlice> *get_converter(json test_spec);
SSTable<KVSlice> *load_sstable(std::string sstable_path, bool load_in_mem);

json run_test(json test_spec) {
  if (test_spec["algo"] == "standard_merge") {
    return run_standard_merge(test_spec);
  } else if (test_spec["algo"] == "learned_merge") {
    return run_learned_merge(test_spec);
  } else if (test_spec["algo"] == "learned_merge_threshold") {
    return run_learned_merge_threshold(test_spec);
  } else if (test_spec["algo"] == "create_input") {
    return create_input_sstable(test_spec);
  } else if (test_spec["algo"] == "sort_join") {
    return run_sort_join(test_spec);
  } else if (test_spec["algo"] == "hash_join") {
    return run_hash_join(test_spec);
  } else if (test_spec["algo"] == "inlj") {
    return run_inlj(test_spec);
  } else if (test_spec["algo"] == "index_study") {
    return run_index_study(test_spec);
  } else if (test_spec["algo"] == "inlj_btree") {
    return run_inlj_with_btree(test_spec);
  } else if (test_spec["algo"] == "inlj_pgm") {
    return run_inlj_with_pgm(test_spec);
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
    SSTable<KVSlice> *keys = generate_uniform_random_distribution(
        num_keys, key_size_bytes, value_size_bytes, comparator,
        result_table_builder);
    if (test_spec.contains("common_keys_file")) {
      SSTable<KVSlice> *common_keys =
          load_sstable(test_spec["common_keys_file"], false);
      // TODO(chesetti): This doesn't have to be inmem.
      SSTableBuilder<KVSlice> *in_mem_result =
          FixedSizeKVInMemSSTableBuilder::InMemMalloc(
              num_keys + common_keys->iterator()->num_elts(), key_size_bytes, value_size_bytes,
              get_comparator(test_spec));
      json merge_log;
      SSTable<KVSlice> *merged_key_list = standardMerge(
          common_keys, keys, comparator, in_mem_result, &merge_log);

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
  } else if (test_spec["method"] == "select_common_keys") {
    std::string source = test_spec["source"];
    uint64_t num_keys_to_select = test_spec["num_keys"];
    int fd = open(source.c_str(), O_RDONLY);
    uint64_t num_keys_in_dataset = get_num_keys_from_sosd_dataset(fd);
    generate_uniform_random_indexes(num_keys_to_select, num_keys_in_dataset,
                                    result_table_builder);
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
  IndexBuilder<KVSlice> *inner_index_builder =
      get_index_builder(test_spec["inner_table"], test_spec);
  Index<KVSlice> *inner_index = build_index(inner_table, inner_index_builder);
  PSSTableBuilder<KVSlice> *result_table_builder =
      get_parallel_result_builder_for_merge(test_spec);
  Comparator<KVSlice> *comparator = get_comparator(test_spec);
  int num_threads = test_spec["num_threads"];

  json merge_log;
  auto merge_start = std::chrono::high_resolution_clock::now();
  auto resultTable = parallelStandardMerge<KVSlice>(
      outer_table, inner_table, inner_index, comparator, num_threads,
      result_table_builder, &merge_log);
  auto merge_end = std::chrono::high_resolution_clock::now();
  auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                         merge_end - merge_start)
                         .count();
  float duration_sec = duration_ns / 1e9;

  result["duration_ns"] = duration_ns;
  result["duration_sec"] = duration_sec;
  result["merge_log"] = merge_log;
  result["checksum"] = md5_checksum(resultTable);

  delete inner_table;
  delete outer_table;
  delete result_table_builder;
  delete comparator;
  return result;
}

json run_sort_join(json test_spec) {
  json result;
  SSTable<KVSlice> *inner_table =
      load_sstable(test_spec["inner_table"], test_spec["load_sstable_in_mem"]);
  SSTable<KVSlice> *outer_table =
      load_sstable(test_spec["outer_table"], test_spec["load_sstable_in_mem"]);
  IndexBuilder<KVSlice> *inner_index_builder =
      get_index_builder(test_spec["inner_table"], test_spec);
  Index<KVSlice> *inner_index = build_index(inner_table, inner_index_builder);
  IndexBuilder<KVSlice> *outer_index_builder =
      get_index_builder(test_spec["outer_table"], test_spec);
  PSSTableBuilder<KVSlice> *result_table_builder =
      get_parallel_result_builder_for_join(test_spec);
  Comparator<KVSlice> *comparator = get_comparator(test_spec);
  int num_threads = test_spec["num_threads"];

  auto merge_start = std::chrono::high_resolution_clock::now();
  SSTable<KVSlice> *resultTable = parallel_presort_join<KVSlice>(
      num_threads, outer_table, inner_table, inner_index,
      comparator, result_table_builder, &result);
  auto merge_end = std::chrono::high_resolution_clock::now();
  auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                         merge_end - merge_start)
                         .count();
  float duration_sec = duration_ns / 1e9;

  result["duration_ns"] = duration_ns;
  result["duration_sec"] = duration_sec;
  result["checksum"] = md5_checksum(resultTable);
  delete inner_table;
  delete outer_table;
  delete result_table_builder;
  delete comparator;

  return result;
}

json run_hash_join(json test_spec) {
  json result;
  SSTable<KVSlice> *inner_table =
      load_sstable(test_spec["inner_table"], test_spec["load_sstable_in_mem"]);
  SSTable<KVSlice> *outer_table =
      load_sstable(test_spec["outer_table"], test_spec["load_sstable_in_mem"]);
  IndexBuilder<KVSlice> *inner_index_builder =
      get_index_builder(test_spec["inner_table"], test_spec);
  Index<KVSlice> *inner_index = build_index(inner_table, inner_index_builder);
  PSSTableBuilder<KVSlice> *result_table_builder =
      get_parallel_result_builder_for_join(test_spec);
  Comparator<KVSlice> *comparator = get_comparator(test_spec);
  int num_threads = test_spec["num_threads"];

  std::unordered_map<std::string, uint64_t> outer_index;
  Iterator<KVSlice> *outer_iter = outer_table->iterator();
  while (outer_iter->valid()) {
    KVSlice kv = outer_iter->key();
    std::string key(kv.data(), kv.key_size_bytes());
    if (outer_index.find(key) == outer_index.end()) {
      outer_index[key] = 0;
    }
    outer_index[key]++;
    outer_iter->next();
  }

  auto merge_start = std::chrono::high_resolution_clock::now();
  auto resultTable = parallel_hash_join<KVSlice>(
      num_threads, outer_table, &outer_index, inner_table,
      inner_index, comparator, result_table_builder, &result);
  auto merge_end = std::chrono::high_resolution_clock::now();
  auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                         merge_end - merge_start)
                         .count();
  float duration_sec = duration_ns / 1e9;

  result["outer_index_size"] =
      outer_index.size() * (uint64_t)test_spec["key_size"];
  result["duration_ns"] = duration_ns;
  result["duration_sec"] = duration_sec;
  result["checksum"] = md5_checksum(resultTable);

  delete inner_table;
  delete outer_table;
  delete result_table_builder;

  return result;
}

json run_learned_merge(json test_spec) {
  json result;
  SSTable<KVSlice> *inner_table =
      load_sstable(test_spec["inner_table"], test_spec["load_sstable_in_mem"]);
  SSTable<KVSlice> *outer_table =
      load_sstable(test_spec["outer_table"], test_spec["load_sstable_in_mem"]);
  IndexBuilder<KVSlice> *inner_index_builder =
      get_index_builder(test_spec["inner_table"], test_spec);
  IndexBuilder<KVSlice> *outer_index_builder =
      get_index_builder(test_spec["outer_table"], test_spec);

  Index<KVSlice> *outer_index = build_index(outer_table, outer_index_builder);
  auto index_build_start = std::chrono::high_resolution_clock::now();
  Index<KVSlice> *inner_index = build_index(inner_table, inner_index_builder);
  auto index_build_end = std::chrono::high_resolution_clock::now();
  auto index_build_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
                         index_build_start - index_build_end)
                         .count();
  float index_build_duration_sec = index_build_duration / 1e9;
  result["inner_index_build_duration_sec"] = index_build_duration_sec;

  Comparator<KVSlice> *comparator = get_comparator(test_spec);
  int num_threads = test_spec["num_threads"];
  PSSTableBuilder<KVSlice> *result_table_builder =
      get_parallel_result_builder_for_merge(test_spec);

  json merge_log;
  auto merge_start = std::chrono::high_resolution_clock::now();
  auto resultTable = parallelLearnedMerge(inner_table, outer_table, inner_index,
                                          outer_index, comparator, num_threads,
                                          result_table_builder, &merge_log);
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
  result["checksum"] = md5_checksum(resultTable);

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

json run_learned_merge_threshold(json test_spec) {
  json result;
  SSTable<KVSlice> *inner_table =
      load_sstable(test_spec["inner_table"], test_spec["load_sstable_in_mem"]);
  SSTable<KVSlice> *outer_table =
      load_sstable(test_spec["outer_table"], test_spec["load_sstable_in_mem"]);
  IndexBuilder<KVSlice> *inner_index_builder =
      get_index_builder(test_spec["inner_table"], test_spec);
  Index<KVSlice> *inner_index = build_index(inner_table, inner_index_builder);
  Comparator<KVSlice> *comparator = get_comparator(test_spec);
  uint64_t threshold = test_spec["threshold"];
  int num_threads = test_spec["num_threads"];
  PSSTableBuilder<KVSlice> *result_table_builder =
      get_parallel_result_builder_for_merge(test_spec);

  json merge_log;
  auto merge_start = std::chrono::high_resolution_clock::now();
  auto resultTable = parallelLearnedMergeWithThreshold(
      outer_table, inner_table, inner_index, threshold, comparator, num_threads,
      result_table_builder, &merge_log);
  auto merge_end = std::chrono::high_resolution_clock::now();
  auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                         merge_end - merge_start)
                         .count();
  float duration_sec = duration_ns / 1e9;

  result["duration_ns"] = duration_ns;
  result["duration_sec"] = duration_sec;
  result["merge_log"] = merge_log;
  result["inner_index_size"] = inner_index->size_in_bytes();
  result["checksum"] = md5_checksum(resultTable);

  delete inner_table;
  delete outer_table;
  delete inner_index_builder;
  delete result_table_builder;
  delete inner_index;
  delete comparator;

  return result;
}

json run_inlj_with_btree(json test_spec) {
  json result;
  SSTable<KVSlice> *inner_table =
      load_sstable(test_spec["inner_table"], test_spec["load_sstable_in_mem"]);
  SSTable<KVSlice> *outer_table =
      load_sstable(test_spec["outer_table"], test_spec["load_sstable_in_mem"]);
  PSSTableBuilder<KVSlice> *result_table_builder =
      get_parallel_result_builder_for_join(test_spec);
  int num_threads = test_spec["num_threads"];
  InnerInMemBTree *btree;

  auto inner_iterator = inner_table->iterator();
  LeafNodeIterm *data = new LeafNodeIterm[inner_iterator->num_elts()];
  uint64_t pos = 0;
  while (inner_iterator->valid()) {
#ifdef STRING_KEYS
      data[pos].key = std::string(inner_iterator->key().data(), (uint64_t)test_spec["key_size"]);
#else
      data[pos].key = *(uint64_t *)(inner_iterator->key().data());
      data[pos].value = *(uint64_t *)(inner_iterator->key().data());
#endif
      inner_iterator->next();
      pos++;
  }
  std::string btree_file_name(test_spec["inner_table"]);
  btree_file_name += "_inmem_btree";
  btree = new InnerInMemBTree(true, btree_file_name.c_str());
  btree->bulk_load(data, pos);

  json merge_log;
  auto merge_start = std::chrono::high_resolution_clock::now();
  SSTable<KVSlice> *resultTable = parallel_indexed_nested_loop_join_with_btree(num_threads, 
      outer_table, inner_table, btree, result_table_builder, &result);
  auto merge_end = std::chrono::high_resolution_clock::now();
  auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                         merge_end - merge_start)
                         .count();
  float duration_sec = duration_ns / 1e9;

  result["duration_ns"] = duration_ns;
  result["duration_sec"] = duration_sec;
  result["merge_log"] = merge_log;
  result["inner_index_size"] = btree->get_inner_size();
  result["checksum"] = md5_checksum(resultTable);

  delete inner_table;
  delete outer_table;
  delete result_table_builder;
  delete btree;

  return result;
}

json run_index_study(json test_spec) {
  json result;
  SSTable<KVSlice> *inner_table =
      load_sstable(test_spec["inner_table"], test_spec["load_sstable_in_mem"]);
  SSTable<KVSlice> *outer_table =
      load_sstable(test_spec["outer_table"], test_spec["load_sstable_in_mem"]);
  IndexBuilder<KVSlice> *inner_index_builder =
      get_index_builder(test_spec["inner_table"], test_spec);
  IndexBuilder<KVSlice> *outer_index_builder =
      get_index_builder(test_spec["outer_table"], test_spec);
  Comparator<KVSlice> *comparator = get_comparator(test_spec);

  auto merge_start = std::chrono::high_resolution_clock::now();
  Index<KVSlice> *outer_index = build_index(outer_table, outer_index_builder);
  auto merge_end = std::chrono::high_resolution_clock::now();
  auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                         merge_end - merge_start)
                         .count();
  float duration_sec = duration_ns / 1e9;
  result["outer_index_build_duration"] = duration_sec;
  result["duration_sec"] = duration_sec;

  merge_start = std::chrono::high_resolution_clock::now();
  Index<KVSlice> *inner_index = build_index(inner_table, inner_index_builder);
  merge_end = std::chrono::high_resolution_clock::now();
  duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                         merge_end - merge_start)
                         .count();
  duration_sec = duration_ns / 1e9;
  result["inner_index_build_duration"] = duration_sec;

  result["inner_index_size"] = inner_index->size_in_bytes();
  result["outer_index_size"] = outer_index->size_in_bytes();
  result["checksum"] = "NA";

  delete inner_table;
  delete outer_table;
  delete inner_index_builder;
  delete outer_index_builder;
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
  IndexBuilder<KVSlice> *inner_index_builder =
      get_index_builder(test_spec["inner_table"], test_spec);
  IndexBuilder<KVSlice> *outer_index_builder =
      get_index_builder(test_spec["outer_table"], test_spec);
  Index<KVSlice> *outer_index = build_index(outer_table, outer_index_builder);
  Index<KVSlice> *inner_index = build_index(inner_table, inner_index_builder);
  Comparator<KVSlice> *comparator = get_comparator(test_spec);
  PSSTableBuilder<KVSlice> *result_table_builder =
      get_parallel_result_builder_for_join(test_spec);
  int num_threads = test_spec["num_threads"];

  json merge_log;
  auto merge_start = std::chrono::high_resolution_clock::now();
  SSTable<KVSlice> *resultTable = parallel_indexed_nested_loop_join<KVSlice>(
      num_threads, outer_table, inner_table, inner_index,
      comparator, result_table_builder, &result);
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
  result["checksum"] = md5_checksum(resultTable);

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

json run_inlj_with_pgm(json test_spec) {
  json result;
  SSTable<KVSlice> *inner_table =
      load_sstable(test_spec["inner_table"], test_spec["load_sstable_in_mem"]);
  SSTable<KVSlice> *outer_table =
      load_sstable(test_spec["outer_table"], test_spec["load_sstable_in_mem"]);
  IndexBuilder<KVSlice> *inner_index_builder =
      get_index_builder(test_spec["inner_table"], test_spec);
  IndexBuilder<KVSlice> *outer_index_builder =
      get_index_builder(test_spec["outer_table"], test_spec);
  Index<KVSlice> *outer_index = build_index(outer_table, outer_index_builder);
  Index<KVSlice> *inner_index = build_index(inner_table, inner_index_builder);
  Comparator<KVSlice> *comparator = get_comparator(test_spec);
  PSSTableBuilder<KVSlice> *result_table_builder =
      get_parallel_result_builder_for_join(test_spec);
  int num_threads = test_spec["num_threads"];

  json merge_log;
  auto merge_start = std::chrono::high_resolution_clock::now();
  SSTable<KVSlice> *resultTable = parallel_indexed_nested_loop_join_with_pgm<KVSlice>(
      num_threads, outer_table, inner_table, inner_index,
      comparator, result_table_builder, &result);
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
  result["checksum"] = md5_checksum(resultTable);

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

IndexBuilder<KVSlice> *get_index_builder(std::string table_path,
                                         json test_spec) {
  if (!test_spec.contains("index")) {
    return new PgmIndexBuilder<KVSlice, 16>(0, get_converter(test_spec));
  }
  std::string index_type = test_spec["index"]["type"];
  if (index_type == "onelevel_pgm8") {
    return new OneLevelPgmIndexBuilder<KVSlice, 8>(0,
                                                    get_converter(test_spec));
  } else if (index_type == "onelevel_pgm64") {
    return new OneLevelPgmIndexBuilder<KVSlice, 64>(0,
                                                    get_converter(test_spec));
  } else if (index_type == "onelevel_pgm128") {
    return new OneLevelPgmIndexBuilder<KVSlice, 128>(0,
                                                     get_converter(test_spec));
  } else if (index_type == "onelevel_pgm256") {
    return new OneLevelPgmIndexBuilder<KVSlice, 256>(0,
                                                     get_converter(test_spec));
  } else if (index_type == "onelevel_pgm1024") {
    return new OneLevelPgmIndexBuilder<KVSlice, 1024>(0,
                                                     get_converter(test_spec));
  } else if (index_type == "onelevel_pgm16") {
    return new OneLevelPgmIndexBuilder<KVSlice, 16>(0,
                                                    get_converter(test_spec));
  } else if (index_type == "onelevel_pgm64") {
    return new OneLevelPgmIndexBuilder<KVSlice, 64>(0,
                                                    get_converter(test_spec));
  } else if (index_type == "onelevel_pgm256") {
    return new OneLevelPgmIndexBuilder<KVSlice, 256>(0,
                                                     get_converter(test_spec));
  } else if (index_type == "pgm8") {
    return new PgmIndexBuilder<KVSlice, 8>(0, get_converter(test_spec));
  } else if (index_type == "pgm16") {
    return new PgmIndexBuilder<KVSlice, 16>(0, get_converter(test_spec));
  } else if (index_type == "pgm64") {
    return new PgmIndexBuilder<KVSlice, 64>(0, get_converter(test_spec));
  } else if (index_type == "pgm128") {
    return new PgmIndexBuilder<KVSlice, 128>(0, get_converter(test_spec));
  } else if (index_type == "pgm256") {
    return new PgmIndexBuilder<KVSlice, 256>(0, get_converter(test_spec));
  } else if (index_type == "rbtree") {
    return new RbTreeIndexBuilder(get_comparator(test_spec),
                                  test_spec["key_size"]);
  } else if (index_type == "betree") {
    return new BeTreeIndexBuilder(table_path + "_betree", 
      test_spec["index"]["cache_size"], 
      test_spec["index"]["node_size"], 
      test_spec["index"]["flush_size"]);
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
