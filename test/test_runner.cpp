#include <gtest/gtest.h>

#include "runner.h"
#include "synthetic.h"
#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace li_merge {

class TestE2E : public ::testing::Test {
private:
  static void create_input_disk_sstable(std::string sstable_path,
                                        uint64_t num_elts, int key_size_bytes,
                                        int value_size_bytes) {
    Comparator<KVSlice> *comparator = new KVSliceMemcmp();
    SSTableBuilder<KVSlice> *builder = new FixedSizeKVDiskSSTableBuilder(
        sstable_path, key_size_bytes, value_size_bytes);
    SSTable<KVSlice> *table = generate_uniform_random_distribution(
        num_elts, key_size_bytes, value_size_bytes, comparator, builder);
    delete comparator;
    delete builder;
    delete table;
  }

public:
  static void SetUpTestSuite() {
    remove("input_1");
    remove("input_2");
    remove("result_e2e");
    remove("result_e2e_inmem");
    create_input_disk_sstable("input_1", 1000, 8, 8);
    create_input_disk_sstable("input_2", 1000000, 8, 8);
  }
  static void TearDownTestSuite() {
    int ret = system("diff result_e2e result_e2e_inmem");
    ASSERT_EQ(ret, 0);
    ret = system("diff result_e2e result_e2e_learned");
    ASSERT_EQ(ret, 0);
  }
};

TEST_F(TestE2E, SM_Disk) {
  json test_spec = json::parse(R"(
    {
        "key_size": 8,
        "value_size": 8,
        "key_type": "str",
        "inner_table": "input_1",
        "outer_table": "input_2",
        "algo": "standard_merge",
        "load_sstable_in_mem": false,
        "write_result_to_disk": true,
        "result_file": "result_e2e"
    }
  )");
  run_test(test_spec);
}

TEST_F(TestE2E, LM_PGM_Disk) {
  json test_spec = json::parse(R"(
    {
        "key_size": 8,
        "value_size": 8,
        "key_type": "str",
        "inner_table": "input_1",
        "outer_table": "input_2",
        "algo": "learned_merge",
        "load_sstable_in_mem": false,
        "write_result_to_disk": true,
        "result_file": "result_e2e_learned"
    }
  )");
  run_test(test_spec);
}

TEST_F(TestE2E, SM_InMem_ResultDisk) {
  json test_spec = json::parse(R"(
    {
        "key_size": 8,
        "value_size": 8,
        "key_type": "str",
        "inner_table": "input_1",
        "outer_table": "input_2",
        "algo": "standard_merge",
        "load_sstable_in_mem": true,
        "write_result_to_disk": true,
        "result_file": "result_e2e_inmem"
    }
  )");
  run_test(test_spec);
}

TEST_F(TestE2E, SM_InMem_ResultInMem) {
  json test_spec = json::parse(R"(
    {
        "key_size": 8,
        "value_size": 8,
        "key_type": "str",
        "inner_table": "input_1",
        "outer_table": "input_2",
        "algo": "standard_merge",
        "load_sstable_in_mem": true,
        "write_result_to_disk": false
    }
  )");
  run_test(test_spec);
}

} // namespace li_merge