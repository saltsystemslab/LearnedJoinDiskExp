#include <gtest/gtest.h>

#include "b_tree.h"
#include "greedy_plr_index.h"
#include "in_mem_sstable.h"
#include "index.h"
#include "key_to_point.h"
#include "one_level_pgm_index.h"
#include "pgm_index.h"
#include "rbtree_index.h"
#include "synthetic.h"
#include <openssl/rand.h>
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

TEST(IndexCreationTest, DISABLED_TestGreedyPLRIndex) {
  uint64_t num_elts = 10000000;
  int key_size_bytes = 8;
  int value_size_bytes = 8;
  Comparator<KVSlice> *comparator = new KVUint64Cmp();
  KeyToPointConverter<KVSlice> *converter = new KVUint64ToDouble();
  SSTable<KVSlice> *table = generate_uniform_random_distribution(
      num_elts, key_size_bytes, value_size_bytes, comparator,
      FixedSizeKVInMemSSTableBuilder::InMemMalloc(num_elts, key_size_bytes,
                                         value_size_bytes, comparator));

  Iterator<KVSlice> *iterator = table->iterator();
  IndexBuilder<KVSlice> *builder = new GreedyPLRIndexBuilder(64, converter);
  Index<KVSlice> *index = build_index_from_iterator(iterator, builder);
  check_index(iterator, index);

  Iterator<KVSlice> *subRangeIterator = table->getSSTableForSubRange(3000000, 4000000)->iterator();
  Index<KVSlice> *subRangeIndex = index->getIndexForSubrange(3000000, 4000000);
  check_index(subRangeIterator, subRangeIndex);
}

TEST(IndexCreationTest, TestRbTree) {
  uint64_t num_elts = 10000000;
  int key_size_bytes = 8;
  int value_size_bytes = 8;
  Comparator<KVSlice> *comparator = new KVUint64Cmp();
  KeyToPointConverter<KVSlice> *converter = new KVUint64ToDouble();
  SSTable<KVSlice> *table = generate_uniform_random_distribution(
      num_elts, key_size_bytes, value_size_bytes, comparator,
      FixedSizeKVInMemSSTableBuilder::InMemMalloc(num_elts, key_size_bytes,
                                         value_size_bytes, comparator));

  Iterator<KVSlice> *iterator = table->iterator();
  IndexBuilder<KVSlice> *builder =
      new RbTreeIndexBuilder(comparator, key_size_bytes);
  Index<KVSlice> *index = build_index_from_iterator(iterator, builder);
  check_index(iterator, index);

  Iterator<KVSlice> *subRangeIterator = table->getSSTableForSubRange(3000000, 4000000)->iterator();
  Index<KVSlice> *subRangeIndex = index->getIndexForSubrange(3000000, 4000000);
  check_index(subRangeIterator, subRangeIndex);
}

TEST(IndexCreationTest, TestPGMUInt64) {
  uint64_t num_elts = 10000000;
  int key_size_bytes = 8;
  int value_size_bytes = 8;
  Comparator<KVSlice> *comparator = new KVUint64Cmp();
  KeyToPointConverter<KVSlice> *converter = new KVUint64ToDouble();
  SSTable<KVSlice> *table = generate_uniform_random_distribution(
      num_elts, key_size_bytes, value_size_bytes, comparator,
      FixedSizeKVInMemSSTableBuilder::InMemMalloc(num_elts, key_size_bytes,
                                         value_size_bytes, comparator));

  Iterator<KVSlice> *iterator = table->iterator();
  IndexBuilder<KVSlice> *builder =
      new PgmIndexBuilder<KVSlice, 64>(10, converter);
  Index<KVSlice> *index = build_index_from_iterator(iterator, builder);
  check_index(iterator, index);

  Iterator<KVSlice> *subRangeIterator = table->getSSTableForSubRange(3000000, 4000000)->iterator();
  Index<KVSlice> *subRangeIndex = index->getIndexForSubrange(3000000, 4000000);
  check_index(subRangeIterator, subRangeIndex);
}

TEST(IndexCreationTest, TestPGM8Byte) {
  uint64_t num_elts = 10000000;
  int key_size_bytes = 8;
  int value_size_bytes = 8;
  Comparator<KVSlice> *comparator = new KVSliceMemcmp();
  KeyToPointConverter<KVSlice> *converter = new KVStringToDouble();
  SSTable<KVSlice> *table = generate_uniform_random_distribution(
      num_elts, key_size_bytes, value_size_bytes, comparator,
      FixedSizeKVInMemSSTableBuilder::InMemMalloc(num_elts, key_size_bytes,
                                         value_size_bytes, comparator));

  Iterator<KVSlice> *iterator = table->iterator();
  IndexBuilder<KVSlice> *builder =
      new PgmIndexBuilder<KVSlice, 64>(10, converter);
  Index<KVSlice> *index = build_index_from_iterator(iterator, builder);
  check_index(iterator, index);

  Iterator<KVSlice> *subRangeIterator = table->getSSTableForSubRange(3000000, 4000000)->iterator();
  Index<KVSlice> *subRangeIndex = index->getIndexForSubrange(3000000, 4000000);
  check_index(subRangeIterator, subRangeIndex);
}

TEST(IndexCreationTest, TestOneLevelPGM8Byte) {
  uint64_t num_elts = 10000000;
  int key_size_bytes = 8;
  int value_size_bytes = 8;
  Comparator<KVSlice> *comparator = new KVSliceMemcmp();
  KeyToPointConverter<KVSlice> *converter = new KVStringToDouble();
  SSTable<KVSlice> *table = generate_uniform_random_distribution(
      num_elts, key_size_bytes, value_size_bytes, comparator,
      FixedSizeKVInMemSSTableBuilder::InMemMalloc(num_elts, key_size_bytes,
                                         value_size_bytes, comparator));

  Iterator<KVSlice> *iterator = table->iterator();
  IndexBuilder<KVSlice> *builder =
      new OneLevelPgmIndexBuilder<KVSlice, 64>(10, converter);
  Index<KVSlice> *index = build_index_from_iterator(iterator, builder);
  check_index(iterator, index);

  Iterator<KVSlice> *subRangeIterator = table->getSSTableForSubRange(3000000, 4000000)->iterator();
  Index<KVSlice> *subRangeIndex = index->getIndexForSubrange(3000000, 4000000);
  check_index(subRangeIterator, subRangeIndex);
}

TEST(IndexCreationTest, TestPGM16Byte) {
  uint64_t num_elts = 10000000;
  int key_size_bytes = 16;
  int value_size_bytes = 8;
  Comparator<KVSlice> *comparator = new KVSliceMemcmp();
  KeyToPointConverter<KVSlice> *converter = new KVStringToDouble();
  SSTable<KVSlice> *table = generate_uniform_random_distribution(
      num_elts, key_size_bytes, value_size_bytes, comparator,
      FixedSizeKVInMemSSTableBuilder::InMemMalloc(num_elts, key_size_bytes,
                                         value_size_bytes, comparator));

  Iterator<KVSlice> *iterator = table->iterator();
  IndexBuilder<KVSlice> *builder =
      new PgmIndexBuilder<KVSlice, 64>(10, converter);
  Index<KVSlice> *index = build_index_from_iterator(iterator, builder);
  check_index(iterator, index);

  Iterator<KVSlice> *subRangeIterator = table->getSSTableForSubRange(3000000, 4000000)->iterator();
  Index<KVSlice> *subRangeIndex = index->getIndexForSubrange(3000000, 4000000);
  check_index(subRangeIterator, subRangeIndex);
}

 void load_keys(std::vector<uint64_t> &keys, LeafNodeIterm *data, int count) {
  for (uint64_t i = 0; i < count; i++) {
    uint64_t k; 
    RAND_bytes((unsigned char *)&k, sizeof(k));
    keys.push_back(k);
  }
  std::sort(keys.begin(), keys.end());
  for (uint64_t i = 0; i < count; i++) {
    data[i].key = keys[i];
    data[i].value = keys[i];
  }
}

void check_tree(std::vector<uint64_t> &keys, BTree *tree) {
  int c;
  for (uint64_t i = 0; i < keys.size(); i++) {
    ASSERT_EQ(tree->lookup(keys[i], &c), true);
    ASSERT_EQ(tree->lookup(keys[i]+1, &c), false);
  }
  for (uint64_t i = 0; i < keys.size()-1; i++) {
    BTree::Condition cond;
    cond.include_min = false;
    cond.min = keys[i];
    cond.max = -1;
    cond.include_max = true;
    auto iter = tree->obtain_for_leaf_disk(cond, &c);
    ASSERT_EQ(iter.cur_key(), keys[i]);
  }

  for (uint64_t i = 0; i < keys.size()-1; i++) {
    BTree::Condition cond;
    cond.include_min = false;
    cond.min = keys[i]-1;
    cond.max = -1;
    cond.include_max = true;
    auto iter = tree->obtain_for_leaf_disk(cond, &c);
    ASSERT_EQ(iter.cur_key(), keys[i]);
  }

  for (uint64_t i = 0; i < keys.size()-1; i++) {
    BTree::Condition cond;
    cond.include_min = false;
    cond.min = keys[i]+1;
    cond.max = -1;
    cond.include_max = true;
    auto iter = tree->obtain_for_leaf_disk(cond, &c);
    ASSERT_EQ(iter.cur_key(), keys[i+1]);
  }

}

TEST(BTreeIndex, DISABLED_TestBTreeIndex_AllDisk_BulkLoad) {
  BTree *tree_;
  tree_ = new BTree(0, true, "alldisk_bulkload", true);
  int count = 2;
  std::vector<uint64_t> keys;
  LeafNodeIterm *data = new LeafNodeIterm[count];
  load_keys(keys, data, count);
  tree_->bulk_load(data, count);
  tree_->sync_metanode();
  check_tree(keys, tree_);
}

TEST(BTreeIndex, DISABLED_TestBTreeIndex_AllDisk_insert) {
  BTree *tree_;
  tree_ = new BTree(0, true, "alldisk_insert", true);
  tree_->sync_metanode();
  int count = 10;
  std::vector<uint64_t> keys;
  LeafNodeIterm *data = new LeafNodeIterm[count];
  load_keys(keys, data, count);
  long long sel;
  long long sml;
  long long inl;
  int update_c = 0;
  int smo_c = 0;
  for (int i=0; i<count; i++) {
	    tree_->insert_key_entry(data[i].key, data[i].value, &sel, &sml, &inl, &smo_c, &update_c);
  }
  tree_->sync_metanode();
  check_tree(keys, tree_);
}

TEST(BTreeIndex, DISABLED_TestBTreeIndex_LeafDisk_insert) { 
  BTree *tree_;
  tree_ = new BTree(1, true, "leafdisk_insert", false);
  tree_->sync_metanode();
  int count = 2;
  std::vector<uint64_t> keys;
  LeafNodeIterm *data = new LeafNodeIterm[count];
  load_keys(keys, data, count);
  long long sel;
  long long sml;
  long long inl;
  int update_c = 0;
  int smo_c = 0;
  for (int i=0; i<count; i++) {
	    tree_->insert_key_entry(data[i].key, data[i].value, &sel, &sml, &inl, &smo_c, &update_c);
  }
  check_tree(keys, tree_);
}

TEST(BTreeIndex, TestBTreeIndex_LeafDisk_BulkLoad) {
  BTree *tree_;
  tree_ = new BTree(1, true, "leafdisk_bulk", true);
  int count = 2000;
  std::vector<uint64_t> keys;
  LeafNodeIterm *data = new LeafNodeIterm[count];
  load_keys(keys, data, count);
  tree_->bulk_load(data, count);
  check_tree(keys, tree_);
}

} // namespace li_merge