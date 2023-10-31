#include <gtest/gtest.h>

#include "in_mem_sstable.h"
#include "index.h"
#include "key_to_point.h"
#include "one_level_pgm_index.h"
#include "btree_index.h"
#include "pgm_index.h"
#include "synthetic.h"
#include <openssl/rand.h>
#include <random>

namespace li_merge {

void check_randomAccess(Iterator<KVSlice> *iterator, Index<KVSlice> *index) {
  uint64_t num_keys = iterator->numElts();
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

void check_index(Iterator<KVSlice> *iterator, Index<KVSlice> *index) {
  uint64_t num_keys = iterator->numElts();
  iterator->seekToFirst();
  uint64_t pos = 0;
  for (uint64_t i = 0; i < num_keys; i++) {
    auto bounds = index->getPositionBounds(iterator->peek(i));
    if (bounds.lower > 20) {
      bounds.lower = 0;
    }
    ASSERT_LE(bounds.lower, pos);
    ASSERT_GE(bounds.upper + 20, pos);
  }
}

TEST(IndexCreationTest, TestBTree1Page) {
  uint64_t numElts = 10000000;
  int key_size_bytes = 8;
  int value_size_bytes = 8;
  Comparator<KVSlice> *comparator = new KVUint64Cmp();
  KeyToPointConverter<KVSlice> *converter = new KVStringToDouble();
  SSTable<KVSlice> *table = generate_uniform_random_distribution(
      numElts, key_size_bytes, value_size_bytes, comparator,
        new FixedSizeKVDiskSSTableBuilder("1page", 8, 8));

  Iterator<KVSlice> *iterator = table->iterator();
  IndexBuilder<KVSlice> *builder =
      new BTreeIndexBuilder(1, 8, 8);
  Index<KVSlice> *index = buildIndexFromIterator(iterator, builder);
  check_index(iterator, index);

  //Iterator<KVSlice> *subRangeIterator =
  //    table->getSSTableForSubRange(3000000, 4000000)->iterator();
  //Index<KVSlice> *subRangeIndex = index->getIndexForSubrange(3000000, 4000000);
  //check_index(subRangeIterator, subRangeIndex);
}

TEST(IndexCreationTest, TestBTree16Page) {
  uint64_t numElts = 1000000;
  int key_size_bytes = 8;
  int value_size_bytes = 8;
  Comparator<KVSlice> *comparator = new KVUint64Cmp();
  SSTable<KVSlice> *table = generate_uniform_random_distribution(
      numElts, key_size_bytes, value_size_bytes, comparator,
        new FixedSizeKVDiskSSTableBuilder("16page", 8, 8));

  Iterator<KVSlice> *iterator = table->iterator();
  IndexBuilder<KVSlice> *builder =
      new BTreeIndexBuilder(16, 8, 8);
  Index<KVSlice> *index = buildIndexFromIterator(iterator, builder);
  check_index(iterator, index);

  /*
  Iterator<KVSlice> *subRangeIterator =
      table->getSSTableForSubRange(3000000, 4000000)->iterator();
  Index<KVSlice> *subRangeIndex = index->getIndexForSubrange(3000000, 4000000);
  check_index(subRangeIterator, subRangeIndex);
  */
}

TEST(IndexCreationTest, TestPGMUInt64) {
  uint64_t numElts = 10000000;
  int key_size_bytes = 8;
  int value_size_bytes = 8;
  Comparator<KVSlice> *comparator = new KVUint64Cmp();
  KeyToPointConverter<KVSlice> *converter = new KVUint64ToDouble();
  SSTable<KVSlice> *table = generate_uniform_random_distribution(
      numElts, key_size_bytes, value_size_bytes, comparator,
      FixedSizeKVInMemSSTableBuilder::InMemMalloc(
          numElts, key_size_bytes, value_size_bytes, comparator));

  Iterator<KVSlice> *iterator = table->iterator();
  IndexBuilder<KVSlice> *builder =
      new PgmIndexBuilder<KVSlice, 64>(10, converter);
  Index<KVSlice> *index = buildIndexFromIterator(iterator, builder);

  Iterator<KVSlice> *subRangeIterator =
      table->getSSTableForSubRange(3000000, 4000000)->iterator();
  Index<KVSlice> *subRangeIndex = index->getIndexForSubrange(3000000, 4000000);
}

TEST(IndexCreationTest, TestPGM8Byte) {
  uint64_t numElts = 10000000;
  int key_size_bytes = 8;
  int value_size_bytes = 8;
  Comparator<KVSlice> *comparator = new KVSliceMemcmp();
  KeyToPointConverter<KVSlice> *converter = new KVStringToDouble();
  SSTable<KVSlice> *table = generate_uniform_random_distribution(
      numElts, key_size_bytes, value_size_bytes, comparator,
      FixedSizeKVInMemSSTableBuilder::InMemMalloc(
          numElts, key_size_bytes, value_size_bytes, comparator));

  Iterator<KVSlice> *iterator = table->iterator();
  IndexBuilder<KVSlice> *builder =
      new PgmIndexBuilder<KVSlice, 64>(10, converter);
  Index<KVSlice> *index = buildIndexFromIterator(iterator, builder);
  check_index(iterator, index);

  Iterator<KVSlice> *subRangeIterator =
      table->getSSTableForSubRange(3000000, 4000000)->iterator();
  Index<KVSlice> *subRangeIndex = index->getIndexForSubrange(3000000, 4000000);
  check_index(subRangeIterator, subRangeIndex);
}

TEST(IndexCreationTest, TestOneLevelPGM8Byte) {
  uint64_t numElts = 10000000;
  int key_size_bytes = 8;
  int value_size_bytes = 8;
  Comparator<KVSlice> *comparator = new KVSliceMemcmp();
  KeyToPointConverter<KVSlice> *converter = new KVStringToDouble();
  SSTable<KVSlice> *table = generate_uniform_random_distribution(
      numElts, key_size_bytes, value_size_bytes, comparator,
      FixedSizeKVInMemSSTableBuilder::InMemMalloc(
          numElts, key_size_bytes, value_size_bytes, comparator));

  Iterator<KVSlice> *iterator = table->iterator();
  IndexBuilder<KVSlice> *builder =
      new OneLevelPgmIndexBuilder<KVSlice, 64>(10, converter);
  Index<KVSlice> *index = buildIndexFromIterator(iterator, builder);
  check_index(iterator, index);

  Iterator<KVSlice> *subRangeIterator =
      table->getSSTableForSubRange(3000000, 4000000)->iterator();
  Index<KVSlice> *subRangeIndex = index->getIndexForSubrange(3000000, 4000000);
  check_index(subRangeIterator, subRangeIndex);
}

TEST(IndexCreationTest, TestPGM16Byte) {
  uint64_t numElts = 10000000;
  int key_size_bytes = 16;
  int value_size_bytes = 8;
  Comparator<KVSlice> *comparator = new KVSliceMemcmp();
  KeyToPointConverter<KVSlice> *converter = new KVStringToDouble();
  SSTable<KVSlice> *table = generate_uniform_random_distribution(
      numElts, key_size_bytes, value_size_bytes, comparator,
      FixedSizeKVInMemSSTableBuilder::InMemMalloc(
          numElts, key_size_bytes, value_size_bytes, comparator));

  Iterator<KVSlice> *iterator = table->iterator();
  IndexBuilder<KVSlice> *builder =
      new PgmIndexBuilder<KVSlice, 64>(10, converter);
  Index<KVSlice> *index = buildIndexFromIterator(iterator, builder);
  check_index(iterator, index);

  Iterator<KVSlice> *subRangeIterator =
      table->getSSTableForSubRange(3000000, 4000000)->iterator();
  Index<KVSlice> *subRangeIndex = index->getIndexForSubrange(3000000, 4000000);
  check_index(subRangeIterator, subRangeIndex);
}

void pad_int_string_with_zeros(std::string &s, int len) {
  while (s.size() < len) {
    s = "0" + s;
  }
}


} // namespace li_merge
