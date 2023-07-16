#include <gtest/gtest.h>

#include "greedy_plr_index.h"
#include "in_mem_sstable.h"
#include "index.h"
#include "key_to_point.h"
#include "pgm_index.h"
#include "rbtree_index.h"
#include "synthetic.h"

namespace li_merge {

void check_index(Iterator<KVSlice> *iterator, Index<KVSlice> *index) {
  iterator->seekToFirst();
  uint64_t pos = 0;
  while (iterator->valid()) {
    auto bounds = index->getPositionBounds(iterator->key());
    ASSERT_LE(bounds.lower, pos);
    ASSERT_GE(bounds.upper, pos);
    iterator->next();
    pos = pos + 1;
  }
}

TEST(IndexCreationTest, DISABLED_TestGreedyPLRIndex) {
  uint64_t num_elts = 10000000;
  int key_size_bytes = 8;
  int value_size_bytes = 8;
  Comparator<KVSlice> *comparator = new KVUint64Cmp();
  KeyToPointConverter<KVSlice> *converter = new KVUint64ToDouble();
  SSTable<KVSlice> *table = generate_uniform_random_distribution(
      num_elts, key_size_bytes, value_size_bytes, comparator,
      new FixedSizeKVInMemSSTableBuilder(num_elts, key_size_bytes,
                                         value_size_bytes, comparator));

  Iterator<KVSlice> *iterator = table->iterator();
  IndexBuilder<KVSlice> *builder = new GreedyPLRIndexBuilder(64, converter);
  Index<KVSlice> *index = build_index_from_iterator(iterator, builder);
  check_index(iterator, index);
}

TEST(IndexCreationTest, TestRbTree) {
  uint64_t num_elts = 10000000;
  int key_size_bytes = 8;
  int value_size_bytes = 8;
  Comparator<KVSlice> *comparator = new KVUint64Cmp();
  KeyToPointConverter<KVSlice> *converter = new KVUint64ToDouble();
  SSTable<KVSlice> *table = generate_uniform_random_distribution(
      num_elts, key_size_bytes, value_size_bytes, comparator,
      new FixedSizeKVInMemSSTableBuilder(num_elts, key_size_bytes,
                                         value_size_bytes, comparator));

  Iterator<KVSlice> *iterator = table->iterator();
  IndexBuilder<KVSlice> *builder =
      new RbTreeIndexBuilder(comparator, key_size_bytes);
  Index<KVSlice> *index = build_index_from_iterator(iterator, builder);
  check_index(iterator, index);
}

TEST(IndexCreationTest, TestPGMUInt64) {
  uint64_t num_elts = 10000000;
  int key_size_bytes = 8;
  int value_size_bytes = 8;
  Comparator<KVSlice> *comparator = new KVUint64Cmp();
  KeyToPointConverter<KVSlice> *converter = new KVUint64ToDouble();
  SSTable<KVSlice> *table = generate_uniform_random_distribution(
      num_elts, key_size_bytes, value_size_bytes, comparator,
      new FixedSizeKVInMemSSTableBuilder(num_elts, key_size_bytes,
                                         value_size_bytes, comparator));

  Iterator<KVSlice> *iterator = table->iterator();
  PgmIndexBuilder<KVSlice> *builder = new PgmIndexBuilder(10, converter);
  Index<KVSlice> *index = build_index_from_iterator(iterator, builder);
  check_index(iterator, index);
}

TEST(IndexCreationTest, TestPGM8Byte) {
  uint64_t num_elts = 10000000;
  int key_size_bytes = 8;
  int value_size_bytes = 8;
  Comparator<KVSlice> *comparator = new KVSliceMemcmp();
  KeyToPointConverter<KVSlice> *converter = new KVStringToDouble();
  SSTable<KVSlice> *table = generate_uniform_random_distribution(
      num_elts, key_size_bytes, value_size_bytes, comparator,
      new FixedSizeKVInMemSSTableBuilder(num_elts, key_size_bytes,
                                         value_size_bytes, comparator));

  Iterator<KVSlice> *iterator = table->iterator();
  PgmIndexBuilder<KVSlice> *builder = new PgmIndexBuilder(10, converter);
  Index<KVSlice> *index = build_index_from_iterator(iterator, builder);
  check_index(iterator, index);
}

TEST(IndexCreationTest, TestPGM16Byte) {
  uint64_t num_elts = 10000000;
  int key_size_bytes = 16;
  int value_size_bytes = 8;
  Comparator<KVSlice> *comparator = new KVSliceMemcmp();
  KeyToPointConverter<KVSlice> *converter = new KVStringToDouble();
  SSTable<KVSlice> *table = generate_uniform_random_distribution(
      num_elts, key_size_bytes, value_size_bytes, comparator,
      new FixedSizeKVInMemSSTableBuilder(num_elts, key_size_bytes,
                                         value_size_bytes, comparator));

  Iterator<KVSlice> *iterator = table->iterator();
  PgmIndexBuilder<KVSlice> *builder = new PgmIndexBuilder(10, converter);
  Index<KVSlice> *index = build_index_from_iterator(iterator, builder);
  check_index(iterator, index);
}

} // namespace li_merge