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
#include "rmi_index.h"
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
json create_input_unsorted_table(json test_spec);
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
    Index<KVSlice> *inner_index = get_index(test_spec["inner_table"], test_spec);
    Comparator<KVSlice> *comparator = get_comparator(test_spec);
    op = new StandardMerge<KVSlice>(outer_table, inner_table, inner_index,
                                    comparator, result_table_builder,
                                    num_threads);
  } else if (test_spec["algo"] == "learned_merge") {
    Index<KVSlice> *inner_index = get_index(test_spec["inner_table"], test_spec);
    Comparator<KVSlice> *comparator = get_comparator(test_spec);
    op = new LearnedMerge1Way<KVSlice>(
        outer_table, inner_table, inner_index, comparator,
        get_search_strategy(test_spec), result_table_builder, num_threads);
  } else if (test_spec["algo"] == "sort_join") {
    Index<KVSlice> *inner_index = get_index(test_spec["inner_table"], test_spec);
    Comparator<KVSlice> *comparator = get_comparator(test_spec);
    op = new SortJoin<KVSlice>(outer_table, inner_table, inner_index,
                               comparator, result_table_builder, num_threads);
  } else if (test_spec["algo"] == "hash_join") {
    Index<KVSlice> *inner_index = get_index(test_spec["inner_table"], test_spec);
    Comparator<KVSlice> *comparator = get_comparator(test_spec);
    op = new HashJoin(outer_table, inner_table, inner_index, comparator,
                      result_table_builder, num_threads);
  } else if (test_spec["algo"] == "lsj") {
    Index<KVSlice> *inner_index = get_index(test_spec["inner_table"], test_spec);
    Comparator<KVSlice> *comparator = get_comparator(test_spec);
    op = new LearnedSortJoin<KVSlice>(
        outer_table, inner_table, inner_index, comparator,
        get_search_strategy(test_spec), result_table_builder, num_threads);
  } else if (test_spec["algo"] == "inlj") {
    Index<KVSlice> *inner_index = get_index(test_spec["inner_table"], test_spec);
    Comparator<KVSlice> *comparator = get_comparator(test_spec);
    op = new Inlj<KVSlice>(outer_table, inner_table, inner_index, comparator,
                           get_search_strategy(test_spec), result_table_builder,
                           num_threads);
  } else if (test_spec["algo"] == "unsorted_lsj") {
    op = new LearnedSortJoinOnUnsortedData<KVSlice>(outer_table, inner_table, result_table_builder, num_threads, test_spec["outer_index_file"]);
  } else if (test_spec["algo"] == "unsorted_inlj") {
    op = new IndexedJoinOnUnsortedData<KVSlice>(outer_table, inner_table, result_table_builder, num_threads, test_spec["inner_index_file"]);
  } else if (test_spec["algo"] == "unsorted_lsj_sorted_output") {
    op = new LearnedSortJoinOnUnsortedDataSortedOutput<KVSlice>(outer_table, inner_table, result_table_builder, num_threads, test_spec["outer_index_file"], test_spec["inner_index_file"]);
  } else if (test_spec["algo"] == "unsorted_inlj_sorted_output") {
    op = new IndexedJoinOnUnsortedDataSortedOutput<KVSlice>(outer_table, inner_table, result_table_builder, num_threads, test_spec["outer_index_file"], test_spec["inner_index_file"]);
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

json create_input_sstable(json test_spec) {
  json result;
  std::string result_path = test_spec["result_path"];
  // Ugh, for sosd, string this is means 1/x * num_keys keys, 
  // for unsorted this is x * num_keys
  uint64_t fraction_of_keys = (uint64_t)test_spec["fraction_of_keys"]; 

  CreateInputTable *createInputTable;
  if (test_spec["method"] == "string") {
    if (fraction_of_keys == 1) {
      uint64_t num_keys = test_spec["num_keys"];
      createInputTable = new Create16ByteStringTables(result_path, num_keys);
    } else {
      std::string source_path = test_spec["source"];
      createInputTable = new Create16ByteStringTables(result_path, source_path, (uint64_t)fraction_of_keys);
    }
  } else if (test_spec["method"] == "sosd") {
    std::string source_path = test_spec["source"];
    createInputTable = new CreateInputTablesFromSosdDataset(source_path, result_path, (uint64_t)fraction_of_keys);
  } else if (test_spec["method"] == "unsorted") {
    std::string source_path = test_spec["source"];
    createInputTable = new CreateUnsortedTable(result_path, source_path, fraction_of_keys);
  } 
  else {
    fprintf(stderr, "Unsupported input creation method!");
    abort();
  }

  SSTable<KVSlice> *table = createInputTable->profileAndCreateInputTable().outputTable;

  if (test_spec["create_indexes"]) {
    CreateIndexes action(result_path, table, get_converter(test_spec));
    result["index_stats"] = action.doAction();
  }
  // printf("Num Keys: %lld\n", table->iterator()->numElts());
  result["num_keys"] = table->iterator()->numElts();
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
  } else if (index_type == "radixspline256") {
    return new RadixSplineIndex<KVSlice>(table_path + "_radixspline256",
                                               get_converter(test_spec), 256);
  } else if (index_type == "radixspline1024") {
    return new RadixSplineIndex<KVSlice>(table_path + "_radixspline1024",
                                               get_converter(test_spec), 1024);
  } else if (index_type == "radixspline4096") {
    return new RadixSplineIndex<KVSlice>(table_path + "_radixspline4096",
                                               get_converter(test_spec), 4096);
  } else if (index_type == "rmi") {
    return new RmiIndex<KVSlice>(test_spec["index"]["dataset"], get_converter(test_spec));
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
  int num_threads = test_spec["num_threads"];
  bool write_result_to_disk = test_spec["write_result_to_disk"];
  int key_size_bytes = test_spec["key_size"];
  int value_size_bytes = test_spec["value_size"];
  if (write_result_to_disk) {
    std::string result_file = test_spec["result_path"];
    if (num_threads == 1) {
      return new PSingleDiskSSTableBuilder(result_file, key_size_bytes, value_size_bytes);
    }
    return new PFixedSizeKVDiskSSTableBuilder(result_file, key_size_bytes,
                                              value_size_bytes);
  } else {
    return new PFixedSizeKVInMemSSTableBuilder(
        0, key_size_bytes, value_size_bytes, get_comparator(test_spec));
  }
}

PSSTableBuilder<KVSlice> *get_parallel_result_builder_for_join(json test_spec) {
  int num_threads = test_spec["num_threads"];
  bool write_result_to_disk = test_spec["write_result_to_disk"];
  int key_size_bytes = test_spec["key_size"];
  int value_size_bytes = test_spec["value_size"];
  if (write_result_to_disk) {
    std::string result_file = test_spec["result_path"];
    if (num_threads == 1) {
      return new PSingleDiskSSTableBuilder(result_file, key_size_bytes, value_size_bytes);
    }
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
