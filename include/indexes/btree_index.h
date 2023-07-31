#ifndef LEARNEDINDEXMERGE_BTREE_INDEX_H
#define LEARNEDINDEXMERGE_BTREE_INDEX_H

#include "b_tree.h"
#include "comparator.h"
#include "key_value_slice.h"
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
      : tree_(tree), key_size_bytes_(key_size_bytes){
        tree_->sync_metanode();
      };
  uint64_t getApproxPosition(const KVSlice &t) override { 
    uint64_t *key = (uint64_t *)(t.data());
    int c;
    BTree::Condition cond;
    cond.include_min = false;
    cond.min = (*key)-1;
    cond.max = -1;
    cond.include_max = true;
    auto iter = tree_->get_index_iterator(cond, &c);
    return iter.cur_value();
  }
  uint64_t getApproxLowerBoundPosition(const KVSlice &t) override { 
    uint64_t *key = (uint64_t *)(t.data());
    int c;
    BTree::Condition cond;
    cond.include_min = false;
    cond.min = (*key)-1;
    cond.max = -1;
    cond.include_max = true;
    auto iter = tree_->get_index_iterator(cond, &c);
    return iter.cur_value();
  }
  Bounds getPositionBounds(const KVSlice &t) override { abort(); }
  uint64_t
  getApproxLowerBoundPositionMonotoneAccess(const KVSlice &t) override {
    uint64_t *key = (uint64_t *)(t.data());
    int c;
    BTree::Condition cond;
    cond.include_min = false;
    cond.min = (*key)-1;
    cond.max = -1;
    cond.include_max = true;
    auto iter = tree_->get_index_iterator(cond, &c);
      return iter.cur_value();
  };
  uint64_t getApproxPositionMonotoneAccess(const KVSlice &t) { 
    uint64_t *key = (uint64_t *)(t.data());
    int c;
    BTree::Condition cond;
    cond.include_min = true;
    cond.min = (*key);
    cond.max = -1;
    cond.include_max = true;
    auto iter = tree_->get_index_iterator(cond, &c);
    return iter.cur_value();
  };
  Bounds getPositionBoundsMonotoneAccess(const KVSlice &t) { abort(); };
  void resetMonotoneAccess() override{};

  uint64_t size_in_bytes() override { 
    return tree_->get_file_size();
  }

private:
  int key_size_bytes_;
  BTree *tree_;
};

class BTreeIndexBuilder : public IndexBuilder<KVSlice> {
public:
  BTreeIndexBuilder(std::string btree_file, int key_size_bytes)
      : num_elts_(0), key_size_bytes_(key_size_bytes) {
    char file_path[btree_file.size() + 1];
    memcpy(file_path, btree_file.c_str(), btree_file.size());
    file_path[btree_file.size()] = '\0';
    tree_ = new BTree(0, true, file_path, true);
  }
  void add(const KVSlice &t) override {
    uint64_t *key = (uint64_t *)(t.data());
    printf("%lu\n", *key);
    elts_.push_back(*key);
    num_elts_++;
  }
  Index<KVSlice> *build() override {
    elts_.push_back(-1);
    LeafNodeIterm *data = new LeafNodeIterm[num_elts_ + 1];
    for (uint64_t i=0; i<num_elts_ + 1; i++) {
      data[i].key = elts_[i];
      data[i].value = i;
    }
    tree_->bulk_load(data, num_elts_+1);
    delete data;
    return new BTreeIndex(tree_, key_size_bytes_);
  }

private:
  int key_size_bytes_;
  uint64_t num_elts_;
  std::vector<uint64_t> elts_;
  BTree *tree_;
};

} // namespace li_merge

#endif