#include <gtest/gtest.h>

#include "in_mem_sstable.h"
#include "sstable.h"
#include "synthetic.h"
#include <inttypes.h>

namespace li_merge {
TEST(SyntheticGen, UniformRandom) {
  uint64_t num_elts = 10;
  int key_size_bytes = 8;
  int value_size_bytes = 8;
  Comparator<KVSlice> *comparator = new KVUint64Cmp();
  SSTable<KVSlice> *table = generate_uniform_random_distribution(
      num_elts, key_size_bytes, value_size_bytes, comparator,
      new FixedSizeKVInMemSSTableBuilder(num_elts, key_size_bytes,
                                         value_size_bytes, comparator));

  Iterator<KVSlice> *iterator = table->iterator();
  iterator->seekToFirst();
  KVSlice lkv = iterator->key();
  iterator->next();
  while (iterator->valid()) {
    KVSlice kv = iterator->key();
    ASSERT_TRUE(comparator->compare(lkv, kv) <= 0);
    lkv = kv;
#if 0
    uint64_t *k = (uint64_t *)kv.data(); 
    uint64_t *v = (uint64_t *)(kv.data() + sizeof(uint64_t)); 
    printf("key: %" PRIu64 " value: %" PRIu64 "\n", *k, *v);
#endif
    iterator->next();
  }
}

TEST(SyntheticGen, DISABLED_UniformRandom_Large) {
  uint64_t num_elts = 100000000;
  int key_size_bytes = 8;
  int value_size_bytes = 8;
  Comparator<KVSlice> *comparator = new KVUint64Cmp();
  SSTable<KVSlice> *table = generate_uniform_random_distribution(
      num_elts, key_size_bytes, value_size_bytes, comparator,
      new FixedSizeKVInMemSSTableBuilder(num_elts, key_size_bytes,
                                         value_size_bytes, comparator));

  Iterator<KVSlice> *iterator = table->iterator();
  iterator->seekToFirst();
  KVSlice lkv = iterator->key();
  iterator->next();
  while (iterator->valid()) {
    KVSlice kv = iterator->key();
    ASSERT_TRUE(comparator->compare(lkv, kv) <= 0);
    lkv = kv;
#if 0
    uint64_t *k = (uint64_t *)kv.data(); 
    uint64_t *v = (uint64_t *)(kv.data() + sizeof(uint64_t)); 
    printf("key: %" PRIu64 " value: %" PRIu64 "\n", *k, *v);
#endif
    iterator->next();
  }
}
} // namespace li_merge