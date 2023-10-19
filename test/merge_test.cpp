#include <gtest/gtest.h>

#include "disk_sstable.h"
#include "in_mem_sstable.h"
#include "join.h"
#include "merge.h"
#include "runner.h"
#include "synthetic.h"
#include <nlohmann/json.hpp>
using json = nlohmann::json;

namespace li_merge {

SSTable<KVSlice> *create_input_inmem(uint64_t *arr, int num_elems) {
  Comparator<KVSlice> *comparator = new KVUint64Cmp();
  SSTableBuilder<KVSlice> *builder =
      FixedSizeKVInMemSSTableBuilder::InMemMalloc(10, 8, 0, comparator);
  for (int i = 0; i < num_elems; i++) {
    builder->add(KVSlice((char *)&arr[i], 8, 0));
  }
  return builder->build();
}

SSTable<KVSlice> *create_input_disk(std::string tableName, uint64_t *arr,
                                    int num_elems) {
  Comparator<KVSlice> *comparator = new KVUint64Cmp();
  SSTableBuilder<KVSlice> *builder =
      new FixedSizeKVDiskSSTableBuilder(tableName, 10, 8, 0, true);
  for (int i = 0; i < num_elems; i++) {
    builder->add(KVSlice((char *)&arr[i], 8, 0));
  }
  return builder->build();
}

void check_output(SSTable<KVSlice> *result, uint64_t *expected, int num_elems) {
  auto iter = result->iterator();
  for (int i = 0; i < num_elems; i++) {
    KVSlice kv = iter->key();
    uint64_t key = *(kv.data());
    ASSERT_EQ(key, expected[i]);
    iter->next();
  }
}

TEST(StandardMerge, StandardMerge) {
  uint64_t inner_data[10] = {10, 20, 30, 40, 50, 60, 70, 80, 90, 100};
  uint64_t outer_data[5] = {25, 45, 65, 85, 105};
  uint64_t expected[15] = {10, 20, 25, 30, 40, 45,  50, 60,
                           65, 70, 80, 85, 90, 100, 105};

  SSTable<KVSlice> *inner = create_input_inmem(inner_data, 10);
  SSTable<KVSlice> *outer = create_input_inmem(outer_data, 5);
  Comparator<KVSlice> *comparator = new KVUint64Cmp();
  SSTableBuilder<KVSlice> *resultBuilder =
      FixedSizeKVInMemSSTableBuilder::InMemMalloc(15, 8, 0, comparator);
  json log;

  SSTable<KVSlice> *result =
      standardMerge(outer, inner, comparator, resultBuilder, &log);
  check_output(result, expected, 15);
}

}; // namespace li_merge