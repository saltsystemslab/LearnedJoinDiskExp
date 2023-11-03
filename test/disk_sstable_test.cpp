#include <gtest/gtest.h>

#include "disk_sstable.h"
#include "synthetic.h"
#include <thread>

namespace li_merge {

void dump_kv(KVSlice *kv) {
  for (int i = 0; i < kv->key_size_bytes() + kv->value_size_bytes(); i++) {
    printf("%02x ", *(unsigned char *)(kv->data() + i));
  }
  printf("\n");
}

TEST(DiskSSTable, TestCreation) {
  uint64_t numElts = 5000000;
  int key_size_bytes = 8;
  int value_size_bytes = 8;
  Comparator<KVSlice> *comparator = new KVSliceMemcmp();
  SSTableBuilder<KVSlice> *builder = new FixedSizeKVDiskSSTableBuilder(
      "test_sstable", key_size_bytes, value_size_bytes);
  char *data = create_uniform_random_distribution_buffer(
      numElts, key_size_bytes, value_size_bytes, comparator);
  char **data_ptrs =
      sort_buffer(data, numElts, key_size_bytes, value_size_bytes, comparator);
  for (uint64_t i = 0; i < numElts; i++) {
    KVSlice kv(data_ptrs[i], key_size_bytes, value_size_bytes);
    builder->add(kv);
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

TEST(PDiskSSTable, PTestCreation_SingleThread) {
  uint64_t numElts = 5000000;
  int key_size_bytes = 8;
  int value_size_bytes = 8;
  Comparator<KVSlice> *comparator = new KVSliceMemcmp();
  PSSTableBuilder<KVSlice> *builder = new PFixedSizeKVDiskSSTableBuilder(
      "test_psstable", key_size_bytes, value_size_bytes);

  char *data = create_uniform_random_distribution_buffer(
      numElts, key_size_bytes, value_size_bytes, comparator);
  char **data_ptrs =
      sort_buffer(data, numElts, key_size_bytes, value_size_bytes, comparator);
  for (int i = 0; i < 4; i++) {
    SSTableBuilder<KVSlice> *p_builder =
        builder->getBuilderForRange(numElts / 4 * i, numElts / 4 * (i + 1));
    uint64_t start = numElts / 4 * i;
    uint64_t end = start + numElts / 4;
    for (uint64_t idx = start; idx < end; idx++) {
      KVSlice kv(data_ptrs[idx], key_size_bytes, value_size_bytes);
      p_builder->add(kv);
    }
    p_builder->build();
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

void add_to_table(SSTableBuilder<KVSlice> *builder, char **data_ptrs,
                  int key_size_bytes, int value_size_bytes, uint64_t start,
                  uint64_t end) {
  for (uint64_t idx = start; idx < end; idx++) {
    KVSlice kv(data_ptrs[idx], key_size_bytes, value_size_bytes);
    builder->add(kv);
  }
  builder->build();
}

TEST(PDiskSSTable, PTestCreation_MultiThread) {
  uint64_t numElts = 5000000;
  int key_size_bytes = 8;
  int value_size_bytes = 8;
  Comparator<KVSlice> *comparator = new KVSliceMemcmp();
  PSSTableBuilder<KVSlice> *builder = new PFixedSizeKVDiskSSTableBuilder(
      "test_psstable_mt", key_size_bytes, value_size_bytes);

  char *data = create_uniform_random_distribution_buffer(
      numElts, key_size_bytes, value_size_bytes, comparator);
  char **data_ptrs =
      sort_buffer(data, numElts, key_size_bytes, value_size_bytes, comparator);
  std::vector<std::thread> threads;
  for (int i = 0; i < 4; i++) {
    SSTableBuilder<KVSlice> *p_builder =
        builder->getBuilderForRange(numElts / 4 * i, numElts / 4 * (i + 1));
    uint64_t start = numElts / 4 * i;
    uint64_t end = start + numElts / 4;
    threads.push_back(std::thread(add_to_table, p_builder, data_ptrs,
                                  key_size_bytes, value_size_bytes, start,
                                  end));
  }
  for (int i = 0; i < 4; i++) {
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

TEST(PDiskSSTable, PTestCreation_MultiThread_Split) {
  uint64_t numElts = 500;
  int key_size_bytes = 8;
  int value_size_bytes = 8;
  Comparator<KVSlice> *comparator = new KVSliceMemcmp();
  PSSTableBuilder<KVSlice> *builder = new PSplitFixedSizeKVDiskSSTableBuilder(
      "test_psstable_mt", key_size_bytes, value_size_bytes);

  char *data = create_uniform_random_distribution_buffer(
      numElts, key_size_bytes, value_size_bytes, comparator);
  char **data_ptrs =
      sort_buffer(data, numElts, key_size_bytes, value_size_bytes, comparator);
  std::vector<char *> expected_data_ptrs;
  std::vector<std::thread> threads;
  for (int i = 0; i < 4; i++) {
    SSTableBuilder<KVSlice> *p_builder =
        builder->getBuilderForRange(numElts / 4 * i, numElts / 4 * (i + 1));
    uint64_t start = numElts / 4 * i + 100;
    uint64_t end = start + numElts / 4 - 100;
    for (int j = start; j < end; j++) {
      expected_data_ptrs.push_back(data_ptrs[j]);
    }
#if 0
    threads.push_back(
      std::thread(add_to_table, p_builder, data_ptrs, key_size_bytes, value_size_bytes, start, end));
#endif
    add_to_table(p_builder, data_ptrs, key_size_bytes, value_size_bytes, start,
                 end);
  }
  SSTable<KVSlice> *table = builder->build();
  Iterator<KVSlice> *iterator = table->iterator();
  ASSERT_EQ(iterator->numElts(), expected_data_ptrs.size());
  uint64_t idx = 0;
  iterator->seekToFirst();
  while (iterator->valid()) {
    KVSlice cur_kv(expected_data_ptrs[idx], key_size_bytes, value_size_bytes);
    ASSERT_TRUE(memcmp(iterator->key().data(), cur_kv.data(),
                       key_size_bytes + value_size_bytes) == 0);

    uint64_t peek_idx = random() % expected_data_ptrs.size();
    KVSlice peek_kv(expected_data_ptrs[peek_idx], key_size_bytes,
                    value_size_bytes);
    ASSERT_TRUE(memcmp(iterator->peek(peek_idx).data(), peek_kv.data(),
                       key_size_bytes + value_size_bytes) == 0);
    iterator->next();
    idx++;
  }
  ASSERT_EQ(idx, expected_data_ptrs.size());

  delete[] data_ptrs;
  delete[] data;
}

TEST(DiskSSTable, DISABLED_TestCreationLarge) {
  uint64_t numElts = 50000000;
  int key_size_bytes = 8;
  int value_size_bytes = 8;
  Comparator<KVSlice> *comparator = new KVSliceMemcmp();
  SSTableBuilder<KVSlice> *builder = new FixedSizeKVDiskSSTableBuilder(
      "test_sstable", key_size_bytes, value_size_bytes);
  char *data = create_uniform_random_distribution_buffer(
      numElts, key_size_bytes, value_size_bytes, comparator);
  char **data_ptrs =
      sort_buffer(data, numElts, key_size_bytes, value_size_bytes, comparator);
  for (uint64_t i = 0; i < numElts; i++) {
    KVSlice kv(data_ptrs[i], key_size_bytes, value_size_bytes);
    builder->add(kv);
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
