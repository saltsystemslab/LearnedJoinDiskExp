#ifndef LEARNEDINDEXMERGE_BTREE_INDEX_H
#define LEARNEDINDEXMERGE_BTREE_INDEX_H


#include "index.h"
#include "comparator.h"
#include "key_value_slice.h"
#include "stx/btree_multimap.h"
#include <map>
#include <algorithm>
#include <vector>

struct traits_inner : stx::btree_default_map_traits<uint64_t, uint64_t> {
    static const bool selfverify = false;
    static const bool debug = false;

    static const int leafslots = 4096 / (sizeof(uint64_t) + sizeof(uint64_t));
    static const int innerslots = 4096 / (sizeof(uint64_t) + sizeof(uint64_t));
};
typedef stx::btree_multimap<uint64_t, uint32_t, std::less<uint64_t>, traits_inner> stx_btree;

namespace li_merge {

class BTreeWIndex : public Index<KVSlice> {
public:
  BTreeWIndex(
    stx_btree *tree, 
    int block_size, 
    int num_blocks,
    uint64_t start_idx=0,
    uint64_t end_idx=-1)
      : tree_(tree), 
      block_size_(block_size), 
      num_blocks_(num_blocks),
      start_idx_(start_idx), end_idx_(end_idx)  
      {};
  Bounds getPositionBounds(const KVSlice &t) override { 
    uint64_t *key = (uint64_t *)(t.data());
    stx_btree::iterator it = tree_->find_for_disk(*key);
    int block_id;
    if (it != tree_->end()) {
      block_id = it.data();
    } else {
      block_id = num_blocks_ - 1;
    }
    uint64_t pos = block_id * block_size_;
    return Bounds {pos, pos + block_size_, pos};
   }

  uint64_t size_in_bytes() override { 
    return tree_->get_tree_size();
  }
  Index<KVSlice> *getIndexForSubrange(uint64_t start, uint64_t end) override {
    return new BTreeWIndex(tree_, block_size_, num_blocks_, start, end);
  }

private:
  uint64_t start_idx_;
  uint64_t end_idx_;
  uint64_t block_size_;
  uint64_t num_blocks_;
  uint64_t num_keys_in_block_;
  stx_btree *tree_;
  std::string btree_file_;
};


class BTreeIndexBuilder : public IndexBuilder<KVSlice> {
public:
  BTreeIndexBuilder()
      : num_elts_(0), block_id(0) {
    tree_ = new stx_btree();
  }
  void add(const KVSlice &t) override {
    #ifdef STRING_KEYS
    elts_.push_back(std::string(t.data(), key_size_bytes_));
    #else
    uint64_t *key = (uint64_t *)(t.data());
    if (num_elts_ > 0 && num_elts_ % num_items_block == 0) {
      elts_.push_back(std::pair<uint64_t, uint32_t>(*key, block_id++));
    }
    #endif
    num_elts_++;
  }
  Index<KVSlice> *build() override {
    tree_->bulk_load(elts_.begin(), elts_.end());
    return new BTreeWIndex(tree_, num_items_block, block_id);
  }

private:
  uint64_t num_elts_;
  uint64_t block_id;
  #ifdef STRING_KEYS
  std::vector<std::string> elts_;
  #else
  std::vector<std::pair<uint64_t, uint32_t>> elts_;
  #endif
  stx_btree *tree_;
  std::string btree_file_;
  int num_items_block = 4096 / (sizeof(uint64_t) + sizeof(uint64_t));
};

} // namespace li_merge

#endif