#ifndef LEARNEDINDEXMERGE_RBTREE_INDEX_H
#define LEARNEDINDEXMERGE_RBTREE_INDEX_H

#include <map>
#include <algorithm>

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
  RbTreeIndex(RbTree *tree, int key_size_bytes, uint64_t start_idx=0, uint64_t end_idx=-1)
      : tree_(tree), key_size_bytes_(key_size_bytes), start_idx_(start_idx), end_idx_(end_idx){};
  uint64_t getApproxPosition(const KVSlice &t) override {
    auto lo = tree_->lower_bound(std::string(t.data(), t.key_size_bytes()));
    uint64_t pos = std::clamp(lo->second, start_idx_, end_idx_);
    return pos - start_idx_;
  }
  uint64_t getApproxLowerBoundPosition(const KVSlice &t) override {
    auto lo = tree_->lower_bound(std::string(t.data(), t.key_size_bytes()));
    uint64_t pos;
    if (lo == tree_->end()) {
      pos = tree_->size();
    } else {
      pos = lo->second;
    }
    pos = std::clamp(pos, start_idx_, end_idx_);
    return pos-start_idx_;
  }
  Bounds getPositionBounds(const KVSlice &t) override {
    auto lo = tree_->lower_bound(std::string(t.data(), t.key_size_bytes()));
    auto hi = tree_->upper_bound(std::string(t.data(), t.key_size_bytes()));
    auto lo_pos = std::clamp(lo->second, start_idx_, end_idx_);
    auto hi_pos = std::clamp(hi->second, start_idx_, end_idx_);
    return Bounds{
      lo_pos - start_idx_,
      lo_pos - start_idx_,
      hi_pos - start_idx_
    };
  }
  uint64_t
  getApproxLowerBoundPositionMonotoneAccess(const KVSlice &t) override {
    return getApproxLowerBoundPosition(t);
  };
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
  Index<KVSlice> *getIndexForSubrange(uint64_t start, uint64_t end) override {
    return new RbTreeIndex(tree_, key_size_bytes_, start, end);
  }

private:
  uint64_t start_idx_;
  uint64_t end_idx_;
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
    tree_->insert(tree_->end(), 
        RbTreeEntry(std::string(t.data(), t.key_size_bytes()), num_elts_));
    num_elts_++;
  }
  Index<KVSlice> *build() override {
    return new RbTreeIndex(tree_, key_size_bytes_);
  }

private:
  int key_size_bytes_;
  uint64_t num_elts_;
  RbTree *tree_;
};

} // namespace li_merge

#endif