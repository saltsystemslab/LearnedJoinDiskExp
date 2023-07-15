#include <gtest/gtest.h>

#include "runner.h"
#include "synthetic.h"
#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace li_merge {

void create_input_disk_sstable(std::string sstable_path, uint64_t num_elts,
                               int key_size_bytes, int value_size_bytes) {
  Comparator<KVSlice> *comparator = new KVSliceMemcmp();
  SSTableBuilder<KVSlice> *builder = new FixedSizeKVDiskSSTableBuilder(
      sstable_path, key_size_bytes, value_size_bytes);
  SSTable<KVSlice> *table = generate_uniform_random_distribution(
      num_elts, key_size_bytes, value_size_bytes, comparator, builder);
  delete comparator;
  delete builder;
  delete table;
}

TEST(TestRunner, TestE2E) {
  create_input_disk_sstable("input_1", 10, 8, 8);
  create_input_disk_sstable("input_2", 10, 8, 8);
  json test_spec;
  test_spec["key_size"] = 8;
  test_spec["value_size"] = 8;
  test_spec["algo"] = "standard_merge";
  test_spec["inner_table"] = "input_1";
  test_spec["outer_table"] = "input_2";
  test_spec["load_sstable_in_mem"] = false;
  test_spec["key_type"] = "str";
  test_spec["write_result_to_disk"] = true;
  test_spec["result_file"] = "result";
  run_test(test_spec);
}

} // namespace li_merge