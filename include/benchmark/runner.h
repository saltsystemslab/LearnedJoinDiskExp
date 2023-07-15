#ifndef LEARNEDMERGE_INDEX_TEST_RUNNER_H
#define LEARNEDMERGE_INDEX_TEST_RUNNER_H

#include "comparator.h"
#include "sstable.h"
#include "key_value_slice.h"
#include "merge.h"
#include "disk_sstable.h"
#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace li_merge {

json run_test(json test_spec);
json run_standard_merge(json test_spec);
json run_learned_merge(json test_spec);
SSTableBuilder<KVSlice> *get_result_builder(json test_spec);
Comparator<KVSlice> *get_comparator(json test_spec);
SSTable<KVSlice> *load_sstable(std::string sstable_path, bool load_in_mem);

json run_test(json test_spec) {
  if (test_spec["algo"] == "standard_merge") {
    return run_standard_merge(test_spec);
  } else if (test_spec["algo"] == "learned_merge") {
    return run_learned_merge(test_spec);
  }
  fprintf(stderr, "Unknown algorithm in testspec!");
  abort();
}

json run_standard_merge(json test_spec) {
  json result;
  SSTable<KVSlice> *inner_table =
      load_sstable(test_spec["inner_table"], test_spec["load_sstable_in_mem"]);
  SSTable<KVSlice> *outer_table =
      load_sstable(test_spec["outer_table"], test_spec["load_sstable_in_mem"]);
  SSTableBuilder<KVSlice> *result_table_builder = get_result_builder(test_spec);
  Comparator<KVSlice> *comparator = get_comparator(test_spec);
  standard_merge<KVSlice>(inner_table, outer_table, comparator, result_table_builder);
  return result;
}

json run_learned_merge(json test_spec) { abort(); }

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

SSTableBuilder<KVSlice> *get_result_builder(json test_spec) {
  bool write_result_to_disk = test_spec["write_result_to_disk"];
  int key_size_bytes = test_spec["key_size"];
  int value_size_bytes = test_spec["value_size"];
  if (write_result_to_disk) {
  std::string result_file = test_spec["result_file"];
    return new FixedSizeKVDiskSSTableBuilder(result_file, key_size_bytes,
                                             value_size_bytes);
  } else {
    return new FixedSizeKVInMemSSTableBuilder(
        0, key_size_bytes, value_size_bytes, get_comparator(test_spec));
  }
}
}

#endif