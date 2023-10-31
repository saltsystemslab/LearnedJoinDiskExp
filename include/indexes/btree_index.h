#ifndef LEARNEDINDEXMERGE_BTREE_INDEX_H
#define LEARNEDINDEXMERGE_BTREE_INDEX_H

#include "comparator.h"
#include "index.h"
#include "key_value_slice.h"
#include "stx/btree_multimap.h"
#include <algorithm>
#include <map>
#include <vector>

struct traits_inner : stx::btree_default_map_traits<uint64_t, uint64_t> {
  static const bool selfverify = false;
  static const bool debug = false;

  static const int leafslots = 4096 / (sizeof(uint64_t) + sizeof(uint64_t));
  static const int innerslots = 4096 / (sizeof(uint64_t) + sizeof(uint64_t));
};
typedef stx::btree_multimap<uint64_t, uint64_t, std::less<uint64_t>,
                            traits_inner>
    stx_btree;

namespace li_merge {

class BTreeWIndex : public Index<KVSlice> {
public:
  BTreeWIndex(stx_btree *tree, int leaf_size_in_keys, int num_blocks,
              uint64_t start_idx = 0, uint64_t end_idx = -1)
      : tree_(tree), leaf_size_in_keys_(leaf_size_in_keys), num_blocks_(num_blocks),
        start_idx_(start_idx), end_idx_(end_idx){

        };
  Bounds getPositionBounds(const KVSlice &t) override {
    uint64_t *key = (uint64_t *)(t.data());
    stx_btree::iterator it = tree_->find_for_disk(*key);
    int block_id;
    if (it != tree_->end()) {
      block_id = it.data();
    } else {
      block_id = num_blocks_;
    }
    uint64_t pos = block_id * leaf_size_in_keys_;
    return Bounds{pos, pos + leaf_size_in_keys_, pos};
  }

  uint64_t sizeInBytes() override { return tree_->get_tree_size(); }
  uint64_t getMaxError() override { return leaf_size_in_keys_; }
  Index<KVSlice> *getIndexForSubrange(uint64_t start, uint64_t end) override {
    return new BTreeWIndex(tree_, leaf_size_in_keys_, num_blocks_, start, end);
  }

private:
  uint64_t start_idx_;
  uint64_t end_idx_;
  uint64_t leaf_size_in_keys_;
  uint64_t num_blocks_;
  uint64_t num_keys_in_block_;
  stx_btree *tree_;
  std::string btree_file_;
};

class BTreeIndexBuilder : public IndexBuilder<KVSlice> {
public:
  BTreeIndexBuilder(int leaf_size_in_pages, int key_size_bytes, int value_size_bytes) : num_elts_(0), block_id(0) { 
    tree_ = new stx_btree(); 
    // TODO(chesetti): Use PAGE_SIZE HERE.
    num_items_block_ = (leaf_size_in_pages * 4096) / (key_size_bytes + value_size_bytes);
  }
  void add(const KVSlice &t) override {
#ifdef STRING_KEYS
    elts_.push_back(std::string(t.data(), key_size_bytes_));
#else
    uint64_t *key = (uint64_t *)(t.data());
    if (num_elts_ > 0 && num_elts_ % num_items_block_ == 0) {
      elts_.push_back(std::pair<uint64_t, uint64_t>(*key, block_id++));
    }
#endif
    num_elts_++;
  }
  Index<KVSlice> *build() override {
    tree_->bulk_load(elts_.begin(), elts_.end());
    return new BTreeWIndex(tree_, num_items_block_, block_id);
  }

private:
  uint64_t num_elts_;
  uint64_t block_id;
#ifdef STRING_KEYS
  std::vector<std::string> elts_;
#else
  std::vector<std::pair<uint64_t, uint64_t>> elts_;
#endif
  std::vector<uint64_t> keys_;
  stx_btree *tree_;
  int num_items_block_;
};

} // namespace li_merge

#endif
