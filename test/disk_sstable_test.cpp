#include <gtest/gtest.h>

#include "disk_sstable.h"
#include "synthetic.h"

namespace li_merge {

void dump_kv(KVSlice *kv) {
  for (int i = 0; i < kv->key_size_bytes() + kv->value_size_bytes(); i++) {
    printf("%02x ", *(unsigned char *)(kv->data() + i));
  }
  printf("\n");
}

TEST(DiskSSTable, TestCreation) {
  uint64_t num_elts = 5000000;
  int key_size_bytes = 8;
  int value_size_bytes = 8;
  Comparator<KVSlice> *comparator = new KVSliceMemcmp();
  SSTableBuilder<KVSlice> *builder = new FixedSizeKVDiskSSTableBuilder(
      "test_sstable", key_size_bytes, value_size_bytes);
  char *data = create_uniform_random_distribution_buffer(
      num_elts, key_size_bytes, value_size_bytes, comparator);
  char **data_ptrs =
      sort_buffer(data, num_elts, key_size_bytes, value_size_bytes, comparator);
  for (uint64_t i = 0; i < num_elts; i++) {
    KVSlice kv(data_ptrs[i], key_size_bytes, value_size_bytes);
    builder->add(kv);
  }
  SSTable<KVSlice> *table = builder->build();
  Iterator<KVSlice> *iterator = table->iterator();
  ASSERT_EQ(iterator->num_elts(), num_elts);
  uint64_t idx = 0;
  iterator->seekToFirst();
  while (iterator->valid()) {
    KVSlice cur_kv(data_ptrs[idx], key_size_bytes, value_size_bytes);
    ASSERT_TRUE(memcmp(iterator->key().data(), cur_kv.data(),
                       key_size_bytes + value_size_bytes) == 0);

    uint64_t peek_idx = random() % num_elts;
    KVSlice peek_kv(data_ptrs[peek_idx], key_size_bytes, value_size_bytes);
    ASSERT_TRUE(memcmp(iterator->peek(peek_idx).data(), peek_kv.data(),
                       key_size_bytes + value_size_bytes) == 0);
    iterator->next();
    idx++;
  }
  ASSERT_EQ(idx, num_elts);

  delete[] data_ptrs;
  delete[] data;
}

TEST(DiskSSTable, DISABLED_TestCreationLarge) {
  uint64_t num_elts = 50000000;
  int key_size_bytes = 8;
  int value_size_bytes = 8;
  Comparator<KVSlice> *comparator = new KVSliceMemcmp();
  SSTableBuilder<KVSlice> *builder = new FixedSizeKVDiskSSTableBuilder(
      "test_sstable", key_size_bytes, value_size_bytes);
  char *data = create_uniform_random_distribution_buffer(
      num_elts, key_size_bytes, value_size_bytes, comparator);
  char **data_ptrs =
      sort_buffer(data, num_elts, key_size_bytes, value_size_bytes, comparator);
  for (uint64_t i = 0; i < num_elts; i++) {
    KVSlice kv(data_ptrs[i], key_size_bytes, value_size_bytes);
    builder->add(kv);
  }
  SSTable<KVSlice> *table = builder->build();
  Iterator<KVSlice> *iterator = table->iterator();
  ASSERT_EQ(iterator->num_elts(), num_elts);
  uint64_t idx = 0;
  iterator->seekToFirst();
  while (iterator->valid()) {
    KVSlice cur_kv(data_ptrs[idx], key_size_bytes, value_size_bytes);
    ASSERT_TRUE(memcmp(iterator->key().data(), cur_kv.data(),
                       key_size_bytes + value_size_bytes) == 0);

    uint64_t peek_idx = random() % num_elts;
    KVSlice peek_kv(data_ptrs[peek_idx], key_size_bytes, value_size_bytes);
    ASSERT_TRUE(memcmp(iterator->peek(peek_idx).data(), peek_kv.data(),
                       key_size_bytes + value_size_bytes) == 0);
    iterator->next();
    idx++;
  }
  ASSERT_EQ(idx, num_elts);

  delete[] data_ptrs;
  delete[] data;
}
}; // namespace li_merge
