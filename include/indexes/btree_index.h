#ifndef LEARNEDINDEXMERGE_BTREE_INDEX_H
#define LEARNEDINDEXMERGE_BTREE_INDEX_H

#include "comparator.h"
#include "index.h"
#include "key_value_slice.h"
#include "stx/btree_map.h"
#include <algorithm>
#include <fstream>
#include <map>
#include <vector>

#ifdef STRING_KEYS
const int KEY_SIZE = 16;
struct KeyStruct {
  char buf[KEY_SIZE];
};
struct KeyStructComp {
  bool operator()(const KeyStruct &a, const KeyStruct &b) const {
    // Define your comparison logic here
    return memcmp(a.buf, b.buf, KEY_SIZE) < 0;
  }
};
typedef KeyStruct KEY_TYPE;
struct traits_inner : stx::btree_default_map_traits<KEY_TYPE, uint64_t> {
  static const bool selfverify = false;
  static const bool debug = false;
  static const int leafslots = 256;
  static const int innerslots = 256;
};
typedef stx::btree_map<KEY_TYPE, uint64_t, KeyStructComp, traits_inner>
    stx_btree;

#else
struct traits_inner : stx::btree_default_map_traits<uint64_t, uint64_t> {
  static const bool selfverify = false;
  static const bool debug = false;

  static const int leafslots = 4096 / (sizeof(uint64_t) + sizeof(uint64_t));
  static const int innerslots = 4096 / (sizeof(uint64_t) + sizeof(uint64_t));
};
typedef uint64_t KEY_TYPE;
typedef stx::btree_map<KEY_TYPE, uint64_t, std::less<KEY_TYPE>, traits_inner>
    stx_btree;
#endif

namespace li_merge {

template <typename T>
static size_t write_member(const T &x, std::ostream &out) {
  out.write((char *)&x, sizeof(T));
  return sizeof(T);
}

template <typename T> static void read_member(T &x, std::istream &in) {
  in.read((char *)&x, sizeof(T));
}

template <typename C>
static size_t write_container(const C &container, std::ostream &out) {
  using value_type = typename C::value_type;
  size_t written_bytes = write_member(container.size(), out);
  for (auto &x : container) {
    out.write((char *)&x, sizeof(value_type));
    written_bytes += sizeof(value_type);
  }
  return written_bytes;
}

template <typename C>
static void read_container(C &container, std::istream &in) {
  using value_type = typename C::value_type;
  typename C::size_type size;
  read_member(size, in);
  container.reserve(size);
  for (size_t i = 0; i < size; ++i) {
    value_type s;
    in.read((char *)&s, sizeof(value_type));
    container.push_back(s);
  }
}

std::vector<std::pair<KEY_TYPE, uint64_t>> loadElts(std::string datafile) {
  std::vector<std::pair<KEY_TYPE, uint64_t>> container;
  std::ifstream inputFile(datafile);
  read_container(container, inputFile);
  return container;
}
void storeElts(std::vector<std::pair<KEY_TYPE, uint64_t>> &data,
               std::string datafile) {
  std::ofstream outputFile(datafile);
  write_container(data, outputFile);
}

class BTreeWIndex : public Index<KVSlice> {
public:
  BTreeWIndex(stx_btree *tree, int leaf_size_in_keys, int num_blocks)
      : tree_(tree), leaf_size_in_keys_(leaf_size_in_keys),
        num_blocks_(num_blocks) {}
  BTreeWIndex(std::string datafile, int leaf_size_in_keys)
      : leaf_size_in_keys_(leaf_size_in_keys) {
    std::vector<std::pair<KEY_TYPE, uint64_t>> elts = loadElts(datafile);
    tree_ = new stx_btree();
    tree_->bulk_load(elts.begin(), elts.end());
    num_blocks_ = elts.size();
  }
  Bounds getPositionBoundsRA(const KVSlice &t) override {
#ifdef STRING_KEYS
    KeyStruct k;
    memcpy(k.buf, t.data(), 16);
    stx_btree::iterator it = tree_->lower_bound(k);
#else
    uint64_t *key = (uint64_t *)(t.data());
    stx_btree::iterator it = tree_->lower_bound(*key);
#endif
    int block_id;
    if (it != tree_->end()) {
      block_id = it.data();
    } else {
      block_id = num_blocks_;
    }
    uint64_t pos = block_id * leaf_size_in_keys_;
    return Bounds{pos, pos + leaf_size_in_keys_, pos};
  }

  Bounds getPositionBounds(const KVSlice &t) override {
#ifdef STRING_KEYS
    KeyStruct k;
    memcpy(k.buf, t.data(), 16);
    stx_btree::iterator it = tree_->lower_bound(k);
#else
    uint64_t *key = (uint64_t *)(t.data());
    stx_btree::iterator it = tree_->lower_bound(*key);
#endif
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
  bool isErrorPageAligned() override { return true; }

private:
  uint64_t leaf_size_in_keys_;
  uint64_t num_blocks_;
  uint64_t num_keys_in_block_;
  stx_btree *tree_;
  std::string btree_file_;
};

class BTreeIndexBuilder : public IndexBuilder<KVSlice> {
public:
  BTreeIndexBuilder(int leaf_size_in_pages, int key_size_bytes,
                    int value_size_bytes)
      : num_elts_(0), block_id(0)  {
    tree_ = new stx_btree();
    // TODO(chesetti): Use PAGE_SIZE HERE.
    num_items_block_ =
        (leaf_size_in_pages * 4096) / (key_size_bytes + value_size_bytes);
  }
  void add(const KVSlice &t) override {
    num_elts_++;
#ifdef STRING_KEYS
    // Copy the keys over.
    KeyStruct k;
    memcpy(k.buf, t.data(), KEY_SIZE);
    if (num_elts_ > 0 && num_elts_ % num_items_block_ == 0) {
      elts_.push_back(std::pair(k, block_id++));
    }
#else
    uint64_t *key = (uint64_t *)(t.data());
    if (num_elts_ > 0 && num_elts_ % num_items_block_ == 0) {
      elts_.push_back(std::pair<uint64_t, uint64_t>(*key, block_id++));
    }
#endif
  }
  Index<KVSlice> *build() override {
    tree_->bulk_load(elts_.begin(), elts_.end());
    return new BTreeWIndex(tree_, num_items_block_, block_id);
  }
  void backToFile(std::string filename) override { 
    storeElts(elts_, filename); 
  }

private:
  uint64_t num_elts_;
  uint64_t block_id;
  std::vector<std::pair<KEY_TYPE, uint64_t>> elts_;
  stx_btree *tree_;
  int num_items_block_;
};

} // namespace li_merge

#endif
