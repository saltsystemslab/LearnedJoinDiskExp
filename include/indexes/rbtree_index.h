#ifndef LEARNEDINDEXMERGE_RBTREE_INDEX_H
#define LEARNEDINDEXMERGE_RBTREE_INDEX_H

#include <map>

namespace li_merge {

class RbTreeComparator {
public:
  RbTreeComparator(Comparator<KVSlice> *comparator, int key_size_bytes)
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
typedef std::map<std::string, uint64_t, RbTreeComparator> RbTree;
typedef std::pair<std::string, uint64_t> RbTreeEntry;

class RbTreeIndex : public Index<KVSlice> {
public:
  RbTreeIndex(RbTree *tree, int key_size_bytes) : tree_(tree), key_size_bytes_(key_size_bytes){};
  uint64_t getApproxPosition(const KVSlice &t) override {
    auto lo = tree_->lower_bound(std::string(t.data(), t.key_size_bytes()));
    return lo->second;
  }
  Bounds getPositionBounds(const KVSlice &t) override {
    auto lo = tree_->lower_bound(std::string(t.data(), t.key_size_bytes()));
    auto pos = lo->second;
    return Bounds{pos, pos, pos};
  }
  uint64_t getApproxPositionMonotoneAccess(const KVSlice &t) {
    return getApproxPosition(t);
  };
  Bounds getPositionBoundsMonotoneAccess(const KVSlice &t) {
    return getPositionBoundsMonotoneAccess(t);
  };
  void resetMonotoneAccess() override{};

  uint64_t size_in_bytes() override {
    return tree_->size() * (24 + key_size_bytes_);
  }

private:
  int key_size_bytes_;
  RbTree *tree_;
};

class RbTreeIndexBuilder : public IndexBuilder<KVSlice> {
public:
  RbTreeIndexBuilder(Comparator<KVSlice> *comp, int key_size_bytes)
      : num_elts_(0), key_size_bytes_(key_size_bytes) {
    tree_ = new RbTree(RbTreeComparator(comp, key_size_bytes));
  }
  void add(const KVSlice &t) override {
    tree_->insert(
        RbTreeEntry(std::string(t.data(), t.key_size_bytes()), num_elts_));
    num_elts_++;
  }
  Index<KVSlice> *build() override { return new RbTreeIndex(tree_, key_size_bytes_); }

private:
  int key_size_bytes_;
  uint64_t num_elts_;
  RbTree *tree_;
};

} // namespace li_merge

#endif