#ifndef LEARNEDINDEXMERGE_BTREE_INDEX_H
#define LEARNEDINDEXMERGE_BTREE_INDEX_H

#include "comparator.h"
#include "key_value_slice.h"
#include "b_tree.h"
#include <map>

namespace li_merge {

class BTreeComparator {
public:
  BTreeComparator(Comparator<KVSlice> *comparator, int key_size_bytes)
      : comparator_(comparator), key_size_bytes_(key_size_bytes) {}
  bool operator()(const std::string &a, const std::string &b) const {
    KVSlice kv1(a.data(), key_size_bytes_, 0);
    KVSlice kv2(b.data(), key_size_bytes_, 0);
    return comparator_->compare(kv1, kv2) < 0;
  }

private:
  Comparator<KVSlice> *comparator_;
  int key_size_bytes_;
};

class BTreeIndex : public Index<KVSlice> {
public:
  BTreeIndex(BTree *tree, int key_size_bytes)
      : tree_(tree), key_size_bytes_(key_size_bytes){};
  uint64_t getApproxPosition(const KVSlice &t) override {
    abort();
  }
  uint64_t getApproxLowerBoundPosition(const KVSlice &t) override {
    abort();
  }
  Bounds getPositionBounds(const KVSlice &t) override {
    abort();
  }
  uint64_t
  getApproxLowerBoundPositionMonotoneAccess(const KVSlice &t) override {
    abort();
  };
  uint64_t getApproxPositionMonotoneAccess(const KVSlice &t) {
    abort();
  };
  Bounds getPositionBoundsMonotoneAccess(const KVSlice &t) {
    abort();
  };
  void resetMonotoneAccess() override{};

  uint64_t size_in_bytes() override {
    abort();
  }

private:
  int key_size_bytes_;
  BTree *tree_;
};

class BTreeIndexBuilder : public IndexBuilder<KVSlice> {
public:
  BTreeIndexBuilder(int key_size_bytes)
      : num_elts_(0), key_size_bytes_(key_size_bytes) {
    tree_ = new BTree(0, true, "btree_index", true);
  }
  void add(const KVSlice &t) override {
    std::string const key(t.data(), key_size_bytes_);
    long long junk_ll;
    int junk_i;
    tree_->insert_key_entry(key, num_elts_++, &junk_ll, &junk_ll, &junk_ll, &junk_i, &junk_i);
  }
  Index<KVSlice> *build() override {
    return new BTreeIndex(tree_, key_size_bytes_);
  }

private:
  int key_size_bytes_;
  uint64_t num_elts_;
  BTree *tree_;
};

} // namespace li_merge

#endif