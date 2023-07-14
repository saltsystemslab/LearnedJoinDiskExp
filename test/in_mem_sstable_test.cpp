#include <gtest/gtest.h>

#include "in_mem_sstable.h"

namespace li_merge {
TEST(InMemSSTable, TestCreation) {
  FixedSizeKVInMemSSTableBuilder *builder =
      new FixedSizeKVInMemSSTableBuilder(0, 2, 0, new KVSliceMemcmp());
  char k1[] = {'a', 'a'};
  char k2[] = {'a', 'b'};
  KVSlice kv1(k1, 2, 0);
  KVSlice kv2(k2, 2, 0);
  builder->add(kv1);
  builder->add(kv2);
  builder->build();
};

TEST(InMemSSTable, TestOutOfOrderInsert) {
  FixedSizeKVInMemSSTableBuilder *builder =
      new FixedSizeKVInMemSSTableBuilder(0, 2, 0, new KVSliceMemcmp());
  char k1[] = {'a', 'a'};
  char k2[] = {'a', 'b'};
  KVSlice kv1(k1, 2, 0);
  KVSlice kv2(k2, 2, 0);
  builder->add(kv2);
  EXPECT_DEATH(builder->add(kv1), "");
}

TEST(InMemSSTable, TestWrongKeySizeInsert) {
  FixedSizeKVInMemSSTableBuilder *builder =
      new FixedSizeKVInMemSSTableBuilder(0, 2, 0, new KVSliceMemcmp());
  char k1[] = {'a', 'a'};
  KVSlice kv1(k1, 3, 0);
  EXPECT_DEATH(builder->add(kv1), "");
}

TEST(InMemSSTable, TestWrongValueSizeInsert) {
  FixedSizeKVInMemSSTableBuilder *builder =
      new FixedSizeKVInMemSSTableBuilder(0, 2, 2, new KVSliceMemcmp());
  char kv1_buf[] = {'a', 'a', 'v', 'e', 'f'};
  KVSlice kv1(kv1_buf, 2, 3);
  EXPECT_DEATH(builder->add(kv1), "");
}
}; // namespace li_merge