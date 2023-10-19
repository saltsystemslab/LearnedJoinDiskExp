#include <gtest/gtest.h>

#include "in_mem_sstable.h"
#include "synthetic.h"
#include <thread>

namespace li_merge {
TEST(InMemSSTable, TestCreation) {
  FixedSizeKVInMemSSTableBuilder *builder =
      FixedSizeKVInMemSSTableBuilder::InMemMalloc(3, 2, 4, new KVSliceMemcmp());
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
  ASSERT_EQ(iterator->numElts(), 3);

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
      FixedSizeKVInMemSSTableBuilder::InMemMalloc(2, 2, 0, new KVSliceMemcmp());
  char k1[] = {'a', 'a'};
  char k2[] = {'a', 'b'};
  KVSlice kv1(k1, 2, 0);
  KVSlice kv2(k2, 2, 0);
  builder->add(kv2);
  EXPECT_DEATH(builder->add(kv1), "");
}

TEST(InMemSSTable, TestWrongKeySizeInsert) {
  FixedSizeKVInMemSSTableBuilder *builder =
      FixedSizeKVInMemSSTableBuilder::InMemMalloc(1, 2, 0, new KVSliceMemcmp());
  char k1[] = {'a', 'a'};
  KVSlice kv1(k1, 3, 0);
  EXPECT_DEATH(builder->add(kv1), "");
}

TEST(InMemSSTable, TestWrongValueSizeInsert) {
  FixedSizeKVInMemSSTableBuilder *builder =
      FixedSizeKVInMemSSTableBuilder::InMemMalloc(1, 2, 2, new KVSliceMemcmp());
  char kv1_buf[] = {'a', 'a', 'v', 'e', 'f'};
  KVSlice kv1(kv1_buf, 2, 3);
  EXPECT_DEATH(builder->add(kv1), "");
}

SSTable<KVSlice> *add_to_table(SSTableBuilder<KVSlice> *builder, char **data_ptrs, int key_size_bytes, int value_size_bytes, uint64_t start, uint64_t end) {
    for (uint64_t idx=start; idx<end; idx++) {
      KVSlice kv(data_ptrs[idx], key_size_bytes, value_size_bytes);
      builder->add(kv);
    }
    return builder->build();
}

TEST(InMemSSTable, TestSubRange) {
  uint64_t numElts = 5000000;
  int key_size_bytes = 8;
  int value_size_bytes = 8;
  Comparator<KVSlice> *comparator = new KVSliceMemcmp();
  SSTableBuilder<KVSlice> *builder = FixedSizeKVInMemSSTableBuilder::InMemMalloc(
      numElts, key_size_bytes, value_size_bytes, comparator);

  char *data = create_uniform_random_distribution_buffer(
      numElts, key_size_bytes, value_size_bytes, comparator);
  char **data_ptrs =
      sort_buffer(data, numElts, key_size_bytes, value_size_bytes, comparator);
  auto table = add_to_table(builder, data_ptrs, key_size_bytes, value_size_bytes, 0, 5000000);

  uint64_t start = 200000;
  uint64_t end = 300000;
  auto subRangeTable = table->getSSTableForSubRange(start, end);
  auto subRangeIter = subRangeTable->iterator();
  for (uint64_t i=start; i<end; i++) {
    KVSlice cur_kv(data_ptrs[i], key_size_bytes, value_size_bytes);
    ASSERT_TRUE(memcmp(subRangeIter->key().data(), cur_kv.data(),
                       key_size_bytes + value_size_bytes) == 0);
    subRangeIter->next();
  }
  ASSERT_EQ(subRangeIter->numElts(), end-start);
  ASSERT_FALSE(subRangeIter->valid());
}

TEST(PInMemSTable, PTestCreation_MultiThread) {
  uint64_t numElts = 5000000;
  int key_size_bytes = 8;
  int value_size_bytes = 8;
  Comparator<KVSlice> *comparator = new KVSliceMemcmp();
  PSSTableBuilder<KVSlice> *builder = new PFixedSizeKVInMemSSTableBuilder(
      numElts, key_size_bytes, value_size_bytes, comparator);

  char *data = create_uniform_random_distribution_buffer(
      numElts, key_size_bytes, value_size_bytes, comparator);
  char **data_ptrs =
      sort_buffer(data, numElts, key_size_bytes, value_size_bytes, comparator);
  std::vector<std::thread> threads;
  for (int i=0; i<4; i++) {
    SSTableBuilder<KVSlice> *p_builder = builder->getBuilderForRange(numElts/4 * i, numElts/4 * (i+1));
    uint64_t start = numElts/4 * i;
    uint64_t end = start + numElts/4;
    threads.push_back(std::thread(add_to_table, p_builder, data_ptrs, key_size_bytes, value_size_bytes, start, end));
  }
  for (int i=0; i<4; i++) {
    threads[i].join();
  }
  SSTable<KVSlice> *table = builder->build();
  Iterator<KVSlice> *iterator = table->iterator();
  ASSERT_EQ(iterator->numElts(), numElts);
  uint64_t idx = 0;
  iterator->seekToFirst();
  while (iterator->valid()) {
    KVSlice cur_kv(data_ptrs[idx], key_size_bytes, value_size_bytes);
    ASSERT_TRUE(memcmp(iterator->key().data(), cur_kv.data(),
                       key_size_bytes + value_size_bytes) == 0);

    uint64_t peek_idx = random() % numElts;
    KVSlice peek_kv(data_ptrs[peek_idx], key_size_bytes, value_size_bytes);
    ASSERT_TRUE(memcmp(iterator->peek(peek_idx).data(), peek_kv.data(),
                       key_size_bytes + value_size_bytes) == 0);
    iterator->next();
    idx++;
  }
  ASSERT_EQ(idx, numElts);

  delete[] data_ptrs;
  delete[] data;
}
}; // namespace li_merge
