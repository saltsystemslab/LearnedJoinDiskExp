#include <gtest/gtest.h>

#include "in_mem_sstable.h"

namespace li_merge {
TEST(InMemSSTable, TestCreation) {
  FixedSizeKVInMemSSTableBuilder *builder =
      new FixedSizeKVInMemSSTableBuilder(0, 2, 4, new KVSliceMemcmp());
  char kvb1[] = {'a', 'a', 'v', '1', '2', '3'};
  char kvb2[] = {'a', 'b', 'v', '2', 'e', 'f'};
  char kvb3[] = {'c', 'b', 'v', '3', '2', 'g'};

  KVSlice kv1(kvb1, 2, 4);
  KVSlice kv2(kvb2, 2, 4);
  KVSlice kv3(kvb3, 2, 4);

  builder->add(kv1);
  builder->add(kv2);
  builder->add(kv3);

  SSTable<KVSlice> *table = builder->build();
  Iterator<KVSlice> *iterator = table->iterator();
  ASSERT_EQ(iterator->num_elts(), 3);

  KVSlice kv;
  iterator->seekToFirst();
  kv = iterator->key();
  ASSERT_EQ(memcmp(kv.data(), kvb1, 6), 0);
  iterator->next();

  kv = iterator->key();
  ASSERT_EQ(memcmp(kv.data(), kvb2, 6), 0);
  iterator->next();

  kv = iterator->key();
  ASSERT_EQ(memcmp(kv.data(), kvb3, 6), 0);
  iterator->next();

  ASSERT_FALSE(iterator->valid());

  kv = iterator->peek(1);
  ASSERT_EQ(memcmp(kv.data(), kvb2, 6), 0);
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