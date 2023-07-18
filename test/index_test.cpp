#include <gtest/gtest.h>

#include "greedy_plr_index.h"
#include "in_mem_sstable.h"
#include "index.h"
#include "key_to_point.h"
#include "one_level_pgm_index.h"
#include "pgm_index.h"
#include "rbtree_index.h"
#include "synthetic.h"
#include <random>

namespace li_merge {

void check_randomAccess(Iterator<KVSlice> *iterator, Index<KVSlice> *index) {
  uint64_t num_keys = iterator->num_elts();
  iterator->seekToFirst();
  uint64_t pos = 0;
  std::random_device rd;  // a seed source for the random number engine
  std::mt19937 gen(rd()); // mersenne_twister_engine seeded with rd()
  std::uniform_int_distribution<uint64_t> distrib(0, num_keys - 1);

  for (uint64_t i = 0; i < num_keys; i++) {
    pos = distrib(gen);
    auto bounds = index->getPositionBounds(iterator->peek(pos));
    if (bounds.lower > 20) {
      bounds.lower = 0;
    }
    ASSERT_LE(bounds.lower, pos);
    ASSERT_GE(bounds.upper + 20, pos);
  }
}

void check_index_monotoneAccess(Iterator<KVSlice> *iterator,
                                Index<KVSlice> *index) {
  iterator->seekToFirst();
  while (iterator->valid()) {
    auto pos = index->getApproxPosition(iterator->key());
    auto posMonotone = index->getApproxPositionMonotoneAccess(iterator->key());
    ASSERT_EQ(pos, posMonotone);
    iterator->next();
  }
}

void check_index(Iterator<KVSlice> *iterator, Index<KVSlice> *index) {
  // check_randomAccess(iterator, index);
  check_index_monotoneAccess(iterator, index);
}

TEST(IndexCreationTest, TestGreedyPLRIndex) {
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
  IndexBuilder<KVSlice> *builder =
      new PgmIndexBuilder<KVSlice, 64>(10, converter);
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
  IndexBuilder<KVSlice> *builder =
      new PgmIndexBuilder<KVSlice, 64>(10, converter);
  Index<KVSlice> *index = build_index_from_iterator(iterator, builder);
  check_index(iterator, index);
}

TEST(IndexCreationTest, TestOneLevelPGM8Byte) {
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
  IndexBuilder<KVSlice> *builder =
      new OneLevelPgmIndexBuilder<KVSlice, 64>(10, converter);
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
  IndexBuilder<KVSlice> *builder =
      new PgmIndexBuilder<KVSlice, 64>(10, converter);
  Index<KVSlice> *index = build_index_from_iterator(iterator, builder);
  check_index(iterator, index);
}

} // namespace li_merge